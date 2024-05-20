/*-------------------------------------------------------------------------
 *
 * tag.c
 *	  Commands to manipulate tag
 *
 * Tags in Cloudberry database are designed to make tag for a given database
 * object.
 *
 *
 * Portions Copyright (c) 2024	Hashdata Inc
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/tag.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_tag.h"
#include "catalog/pg_tag_description.h"
#include "catalog/pg_type.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "commands/tag.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#define TAG_VALUES_ACTION_ADD	1
#define TAG_VALUES_ACTION_DROP	-1

static Datum transformTagAllowedValues(int action, Datum oldvalues, List *allowed_values);
static List *untransformTagValues(Datum allowed_values);
static Datum valueListToArray(List *allowed_values);

/*
 * get_tag_oid - given a tag name, look up the OID
 *
 * If missing_ok is false, throw an error if tag name not found.  If
 * true, just return InvalidOid.
 */
Oid
get_tag_oid(const char *tagname, bool missing_ok)
{
	Oid 		oid;
	
	/*
	 * Search pg_tag syscache with name to get oid.
	 */
	oid = GetSysCacheOid1(TAGNAME, Anum_pg_tag_oid, CStringGetDatum(tagname));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tag \"%s\" does not exist", tagname)));
	
	return oid;
}

/*
 * Create a tag
 * 
 * We will record the create user as tag's owner, which is allowed to
 * alter or drop this tag.
 */
ObjectAddress
CreateTag(CreateTagStmt *stmt)
{
	ObjectAddress	myself = {0};
	Relation	rel;
	Datum		values[Natts_pg_tag];
	bool		nulls[Natts_pg_tag];
	HeapTuple	tuple;
	Oid			tagId;
	Oid			ownerId;
	Datum		tag_values;

	rel = table_open(TagRelationId, RowExclusiveLock);

	/* For now the owner cannot be specified on create. Use effective user ID. */
	ownerId = GetUserId();

	tuple = SearchSysCache1(TAGNAME, CStringGetDatum(stmt->tag_name));
	if (HeapTupleIsValid(tuple) && !stmt->missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("tag \"%s\" already exists", stmt->tag_name)));

	/*
	 * Insert tuple into pg_tag.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	tagId = GetNewOidForTag(rel, TagOidIndexId,
						 	Anum_pg_tag_oid,
						 	stmt->tag_name);
	values[Anum_pg_tag_oid - 1] = ObjectIdGetDatum(tagId);
	values[Anum_pg_tag_tagname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->tag_name));
	values[Anum_pg_tag_tagowner - 1] =
		ObjectIdGetDatum(ownerId);

	tag_values = transformTagAllowedValues(TAG_VALUES_ACTION_ADD,
										   PointerGetDatum(NULL),
										   stmt->allowed_values);

	if (PointerIsValid(DatumGetPointer(tag_values)))
		values[Anum_pg_tag_allowed_values - 1] = tag_values;
	else
		nulls[Anum_pg_tag_allowed_values - 1] = true;

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	/* Post creation hook for new tag */
	InvokeObjectPostCreateHook(TagRelationId, tagId, 0);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
							  		DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
							  		GetAssignedOidsForDispatch(),
							  		NULL);
	}

	table_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * Execute ALTER TAG RENAME
 */
ObjectAddress
RenameTag(const char *oldname, const char *newname)
{
	Oid 		tagId;
	Relation 	rel;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Form_pg_tag tagform;
	ObjectAddress	address;
	
	/*
	 * Look up the target tag's OID, and get exclusive lock on it. We
	 * need this for this same reasons as DROP TAG.
	 */
	rel = table_open(TagRelationId, RowExclusiveLock);

	newtuple = SearchSysCacheCopy1(TAGNAME, CStringGetDatum(oldname));
	if (!HeapTupleIsValid(newtuple))
		elog(ERROR, "cache lookup failed for tag %s", oldname);
	
	tagform = (Form_pg_tag) GETSTRUCT(newtuple);
	tagId = tagform->oid;
	
	/* check permission on tag */
	if (!pg_tag_ownercheck(tagId, GetUserId()))
		aclcheck_error_type(ACLCHECK_NOT_OWNER, tagId);

	tuple = SearchSysCache1(TAGNAME, CStringGetDatum(newname));
	if (HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("tag \"%s\" already exists",
				 newname)));
	
	/* OK, update the entry */
	namestrcpy(&(tagform->tagname), newname);
	
	CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
	
	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(TagRelationId,
					 	   tagId,
					 	   GetUserId(),
					 	   "ALTER", "RENAME");

	InvokeObjectPostAlterHook(TagRelationId, tagId, 0);
	
	ObjectAddressSet(address, TagRelationId, tagId);

	heap_freetuple(newtuple);
	table_close(rel, RowExclusiveLock);
	
	return address;
}

static Datum
transformTagAllowedValues(int action, Datum oldvalues,
						  List *allowed_values)
{
	List		*resultValues = untransformTagValues(oldvalues);
	ListCell	*value_cell;
	Datum		result;
	
	foreach(value_cell, allowed_values)
	{
		char	*value = strVal(lfirst(value_cell));
		ListCell	*cell;

		foreach(cell, resultValues)
		{
			char	*oldvalue = strVal(lfirst(cell));
			
			if (strcmp(value, oldvalue) == 0)
				break;
		}
		
		if (action > 0)
		{
			if (cell)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("allowed value \"%s\" has been added",
								strVal(lfirst(value_cell)))));

			resultValues = lappend(resultValues, lfirst(value_cell));
		}
		else
		{
			if (!cell)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("allowed value \"%s\" not found",
								strVal(lfirst(value_cell)))));
			
			resultValues = list_delete_cell(resultValues, cell);
		}
	}

	/*
	 * Max allowed_values number is 256.
	 */
	if (list_length(resultValues) > 256)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Allowed_values only allow 256 values.")));
	
	result = valueListToArray(resultValues);
	
	return result;
}

static List *
untransformTagValues(Datum allowed_values)
{
	List		*result = NIL;
	ArrayType	*array;
	Datum		*valuedatums;
	int			nvalues;
	int			i;
	
	/* Nothing to do if no allowed_values */
	if (!PointerIsValid(DatumGetPointer(allowed_values)))
		return result;
	
	array = DatumGetArrayTypeP(allowed_values);

	deconstruct_array(array, TEXTOID, -1, false, TYPALIGN_INT,
					  &valuedatums, NULL, &nvalues);
	
	for (i = 0; i < nvalues; i ++)
	{
		char	*s;
		Node	*val = NULL;
		
		s = TextDatumGetCString(valuedatums[i]);
		val = (Node *) makeString(pstrdup(s));
		
		result = lappend(result, val);
	}
	
	return result;
}

static Datum
valueListToArray(List *allowed_values)
{
	ArrayBuildState	*astate = NULL;
	ListCell	*cell;
	
	foreach(cell, allowed_values)
	{
		char	*value = strVal(lfirst(cell));
		Size	len;
		text	*t;

		len = VARHDRSZ + strlen(value);
		t = palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "%s", value);
		
		astate = accumArrayResult(astate, PointerGetDatum(t),
								  false, TEXTOID,
								  CurrentMemoryContext);
	}

	if (astate)
		return makeArrayResult(astate, CurrentMemoryContext);
	
	return PointerGetDatum(NULL);
}
