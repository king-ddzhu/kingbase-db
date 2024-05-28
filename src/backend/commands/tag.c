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

#include "access/genam.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_tag.h"
#include "catalog/pg_tag_description.h"
#include "catalog/pg_type.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "commands/tag.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#define TAG_VALUES_ACTION_ADD	1
#define TAG_VALUES_ACTION_DROP	-1

static Datum transformTagValues(int action, Datum oldvalues, List *allowed_values);
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
Oid
CreateTag(CreateTagStmt *stmt)
{
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
	if (HeapTupleIsValid(tuple))
	{
		if (stmt->missing_ok)
		{
			ereport(NOTICE, 
							errmsg("tag \"%s\" already exists, skipping",
			  				stmt->tag_name));
			ReleaseSysCache(tuple);
			table_close(rel, RowExclusiveLock);
			
			return InvalidOid;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("tag \"%s\" already exists", stmt->tag_name)));
		}
	}

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

	tag_values = transformTagValues(TAG_VALUES_ACTION_ADD,
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

	return tagId;
}

/*
 * Alter a tag
 * 
 * We can alter a tag to add, drop or unset allowed_values.
 */
ObjectAddress
AlterTag(AlterTagStmt *stmt)
{
	Relation	rel;
	HeapTuple	tuple;
	Datum		repl_val[Natts_pg_tag];
	bool		repl_null[Natts_pg_tag];
	bool		repl_repl[Natts_pg_tag];
	Oid			tagId;
	Datum		datum;
	bool		isnull;
	Form_pg_tag tagform;
	ObjectAddress	address = {0};

	rel = table_open(TagRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopy1(TAGNAME, CStringGetDatum(stmt->tag_name));
	if (!HeapTupleIsValid(tuple))
	{
		if (stmt->missing_ok)
		{
			ereport(NOTICE,
						errmsg("tag \"%s\" does not exist, skipping",
						stmt->tag_name));
			table_close(rel, RowExclusiveLock);
			
			return InvalidObjectAddress;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tag \"%s\" does not exist",
			 		 stmt->tag_name)));
		}
	}

	tagform = (Form_pg_tag) GETSTRUCT(tuple);
	tagId = tagform->oid;

	/*
	 * Only owner or a superuser can ALTER a TAG.
	 */
	if (!pg_tag_ownercheck(tagId, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TAG,
				 	   stmt->tag_name);

	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	if (stmt->tag_values)
	{
		/* Extract the current allowed_values */
		datum = SysCacheGetAttr(TAGOID,
								tuple,
								Anum_pg_tag_allowed_values,
								&isnull);

		if (isnull)
			datum = PointerGetDatum(NULL);

		/* Prepare the values array */
		datum = transformTagValues(stmt->action,
								   datum,
								   stmt->tag_values);
	}
	
	if (stmt->unset)
	{
		datum = PointerGetDatum(NULL);
	}

	if (PointerIsValid(DatumGetPointer(datum)))
		repl_val[Anum_pg_tag_allowed_values - 1] = datum;
	else
		repl_null[Anum_pg_tag_allowed_values - 1] = true;

	repl_repl[Anum_pg_tag_allowed_values - 1] = true;
	
	/* Everything looks good - update the tuple */
	tuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
						      repl_val, repl_null, repl_repl);
	
	CatalogTupleUpdate(rel, &tuple->t_self, tuple);
	
	InvokeObjectPostAlterHook(TagRelationId, tagId, 0);
	
	ObjectAddressSet(address, TagRelationId, tagId);
	
	heap_freetuple(tuple);
	
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
							  		DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
							  		GetAssignedOidsForDispatch(),
							  		NULL);
	}

	table_close(rel, RowExclusiveLock);

	return address;
}

/*
 * Drop a tag
 */
void
DropTag(DropTagStmt *stmt)
{
	Relation	rel;
	Oid			curserid;
	ListCell	*cell;
	
	curserid = GetUserId();
	
	rel = table_open(TagRelationId, RowExclusiveLock);
	
	foreach(cell, stmt->tags)
	{
		HeapTuple	tuple;
		Form_pg_tag tagform;
		char		*detail;
		char		*detail_log;
		Oid			tagId;
		char		*tagname;

		/*
		 * Check that if the tag exists. Do nothing if IF NOT
		 * EXISTS was enforced.
		 */
		tagname = strVal(lfirst(cell));
		tuple = SearchSysCache1(TAGNAME, PointerGetDatum(tagname));
		if (!HeapTupleIsValid(tuple))
		{
			if (!stmt->missing_ok)
			{
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("tag \"%s\" does not exist", tagname)));
			}
			if (Gp_role != GP_ROLE_EXECUTE)
			{
				ereport(NOTICE,
						(errmsg("tag \"%s\" does not exist, skipping", tagname)));
			}
			
			continue;
		}

		/* check permission on tag */
		if (!pg_tag_ownercheck(tagId, GetUserId()))
			aclcheck_error_type(ACLCHECK_NOT_OWNER, tagId);

		tagform = (Form_pg_tag) GETSTRUCT(tuple);
		tagId = tagform->oid;

		/* DROP hook for the tag being removed */
		InvokeObjectDropHook(TagRelationId, tagId, 0);
		
		/*
		 * Lock the tag, so nobody can add dependencies to it while we drop
		 * it. We keep the lock until the end of transaction.
		 */
		LockSharedObject(TagRelationId, tagId, 0, AccessExclusiveLock);
		
		/* Check for pg_shdepend entries depending on this tag */
		if (checkSharedDependencies(TagRelationId, tagId,
									&detail, &detail_log))
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("tag \"%s\" cannot be dropped because some objects depend on it",
									tagname),
					 errdetail_internal("%s", detail),
					 errdetail_log("%s", detail_log)));
		
		/*
		 * Delete the tag from the pg_tag table
		 */
		CatalogTupleDelete(rel, &tuple->t_self);
		
		ReleaseSysCache(tuple);
		
		/* metadata track */
		if (Gp_role == GP_ROLE_DISPATCH)
			MetaTrackDropObject(TagRelationId, tagId);
	}
	
	/*
	 * Now we can clean up; but keep locks until commit.
	 */
	table_close(rel, NoLock);
	
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
										DF_WITH_SNAPSHOT|
										DF_NEED_TWO_PHASE,
									NIL,
									NULL);
	}
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

/*
 * Add tag for object
 *
 * Add tag for database object such as database, warehouse, table etc.
 */
void
AddTagDescriptions(List *tags,
				   Oid classid,
				   Oid objid,
				   int32 objsubid,
				   char *objname)
{
	Relation	tag_rel;
	Relation	tag_desc_rel;
	ListCell	*cell;

	tag_rel = table_open(TagRelationId, RowExclusiveLock);
	tag_desc_rel = table_open(TagDescriptionRelationId, RowExclusiveLock);

	foreach(cell, tags)
	{
		Oid		tag_desc_oid;
		DefElem *def;
		HeapTuple	tuple;
		HeapTuple	desc_tuple;
		HeapTuple	new_tuple;
		Form_pg_tag tagform;
		Oid		tagId;
		char	*tagname;
		char	*tagvalue;
		Datum	datum;
		bool	isnull;
		List	*allowed_values;
		ListCell	*value_cell;
		
		def = lfirst(cell);
		tagname = def->defname;
		tagvalue = defGetString(def);
		
		tuple = SearchSysCache1(TAGNAME, CStringGetDatum(tagname));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for tag %s", tagname);

		tagform = (Form_pg_tag) GETSTRUCT(tuple);
		tagId = tagform->oid;
		
		/* Extract the current tag's allowed_values */
		datum = SysCacheGetAttr(TAGNAME,
								tuple,
								Anum_pg_tag_allowed_values,
								&isnull);

		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tag value \"%s\" does not exist in tag \"%s\"",
							tagvalue, tagname)));

		allowed_values = untransformTagValues(datum);

		foreach(value_cell, allowed_values)
		{
			char	*allowed_value = strVal(lfirst(value_cell));

			if (strcmp(tagvalue, allowed_value) == 0)
			{
				break;
			}
		}

		if (value_cell)
		{
			desc_tuple = SearchSysCache4(TAGDESCRIPTION,
										 ObjectIdGetDatum(classid),
										 ObjectIdGetDatum(objid),
										 Int32GetDatum(objsubid),
										 ObjectIdGetDatum(tagId));
			
			if (!HeapTupleIsValid(desc_tuple))
			{
				Datum	tag_desc_values[Natts_pg_tag_description];
				bool	tag_desc_nulls[Natts_pg_tag_description];
				
				/*
				 * Insert tuple into pg_tag_description and record dependency.
				 */
				memset(tag_desc_values, 0, sizeof(tag_desc_values));
				memset(tag_desc_nulls, false, sizeof(tag_desc_nulls));

				tag_desc_oid = GetNewOidForTagDescription(tag_desc_rel, TagDescriptionOidIndexId,
														  Anum_pg_tag_description_oid,
														  objname,
														  tagId);
				tag_desc_values[Anum_pg_tag_description_oid - 1] = ObjectIdGetDatum(tag_desc_oid);
				tag_desc_values[Anum_pg_tag_description_classid - 1] = ObjectIdGetDatum(classid);
				tag_desc_values[Anum_pg_tag_description_objid - 1] = ObjectIdGetDatum(objid);
				tag_desc_values[Anum_pg_tag_description_objsubid - 1] = Int32GetDatum(objsubid);
				tag_desc_values[Anum_pg_tag_description_tagid - 1] = ObjectIdGetDatum(tagId);

				/* Prepare the values array */
				datum = transformTagValues(TAG_VALUES_ACTION_ADD,
										   PointerGetDatum(NULL),
										   list_make1(makeString(tagvalue)));
				if (PointerIsValid(DatumGetPointer(datum)))
					tag_desc_values[Anum_pg_tag_description_tagvalue - 1] = datum;
				else
					tag_desc_nulls[Anum_pg_tag_description_tagvalue - 1] = true;

				new_tuple = heap_form_tuple(tag_desc_rel->rd_att, tag_desc_values, tag_desc_nulls);

				CatalogTupleInsert(tag_desc_rel, new_tuple);

				heap_freetuple(new_tuple);

				/* Record tag dependency */
				recordTagDependency(TagDescriptionRelationId, tag_desc_oid, tagId);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("tag \"%s\" value has been added for object \"%s\".",
			  					tagname, objname)));
//				Datum	tag_desc_repl_val[Natts_pg_tag_description];
//				bool	tag_desc_repl_null[Natts_pg_tag_description];
//				bool	tag_desc_repl_repl[Natts_pg_tag_description];
//				
//				/*
//				 * Update existing tuple in pg_tag_description
//				 */
//				memset(tag_desc_repl_val, 0, sizeof(tag_desc_repl_val));
//				memset(tag_desc_repl_null, false, sizeof(tag_desc_repl_null));
//				memset(tag_desc_repl_repl, false, sizeof(tag_desc_repl_repl));
//
//				/* Extract the current tagvalues */
//				datum = SysCacheGetAttr(TAGDESCRIPTION,
//										desc_tuple,
//										Anum_pg_tag_description_tagvalues,
//										&isnull);
//
//				if (isnull)
//					datum = PointerGetDatum(NULL);
//
//				/* Prepare the values array */
//				datum = transformTagValues(TAG_VALUES_ACTION_ADD,
//										   datum,
//										   list_make1(makeString(tagvalue)));
//
//				if (PointerIsValid(DatumGetPointer(datum)))
//					tag_desc_repl_val[Anum_pg_tag_description_tagvalues - 1] = datum;
//				else
//					tag_desc_repl_null[Anum_pg_tag_description_tagvalues - 1] = true;
//
//				tag_desc_repl_repl[Anum_pg_tag_description_tagvalues - 1] = true;
//
//				/* Everything looks good - update the tuple */
//				new_tuple = heap_modify_tuple(desc_tuple, RelationGetDescr(tag_desc_rel),
//										  	  tag_desc_repl_val, tag_desc_repl_null, tag_desc_repl_repl);
//
//				CatalogTupleUpdate(tag_desc_rel, &new_tuple->t_self, new_tuple);
//				
//				heap_freetuple(new_tuple);
//
//				ReleaseSysCache(desc_tuple);
			}
		}
		else
		{
			ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("tag value \"%s\" is not in tag \"%s\" allowed values",
								tagvalue, tagname)));
		}

		ReleaseSysCache(tuple);
		
		CommandCounterIncrement();
	}
	
	table_close(tag_desc_rel, RowExclusiveLock);
	table_close(tag_rel, NoLock);
	
	return;
}

/*
 * Alter tag for object
 *
 * Alter tag for database object such as database, warehouse, table etc.
 */
void
AlterTagDescriptions(List *tags,
					 Oid classid,
					 Oid objid,
					 int32 objsubid,
					 char *objname)
{
	Relation	tag_rel;
	Relation	tag_desc_rel;
	ListCell	*cell;

	tag_rel = table_open(TagRelationId, RowExclusiveLock);
	tag_desc_rel = table_open(TagDescriptionRelationId, RowExclusiveLock);

	foreach(cell, tags)
	{
		HeapTuple	tuple;
		HeapTuple	desc_tuple;
		HeapTuple	new_tuple;
		Form_pg_tag	tagform;
		Oid		tagId;
		char	*tagname = NULL;
		char	*tagvalue = NULL;
		Datum	datum;
		bool	isnull;
		List	*allowed_values;
		ListCell	*value_cell;
		Datum	tag_desc_repl_val[Natts_pg_tag_description];
		bool	tag_desc_repl_null[Natts_pg_tag_description];
		bool	tag_desc_repl_repl[Natts_pg_tag_description];

		DefElem	*def = lfirst(cell);
		tagname = def->defname;
		tagvalue = defGetString(def);

		tuple = SearchSysCache1(TAGNAME, CStringGetDatum(tagname));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for tag %s", tagname);

		/* Extract the current tag's allowed_values */
		datum = SysCacheGetAttr(TAGNAME,
								tuple,
								Anum_pg_tag_allowed_values,
								&isnull);

		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tag value \"%s\" does not exist in tag \"%s\"",
							tagvalue, tagname)));

		allowed_values = untransformTagValues(datum);

		foreach(value_cell, allowed_values)
		{
			char	*allowed_value = strVal(lfirst(value_cell));

			if (strcmp(tagvalue, allowed_value) == 0)
			{
				break;
			}
		}

		if (!value_cell)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tag value \"%s\" is not in tag \"%s\" allowed values",
							tagvalue, tagname)));
		}
		
		tagform = (Form_pg_tag) GETSTRUCT(tuple);
		tagId = tagform->oid;

		desc_tuple = SearchSysCache4(TAGDESCRIPTION,
									 ObjectIdGetDatum(classid),
									 ObjectIdGetDatum(objid),
									 Int32GetDatum(objsubid),
									 ObjectIdGetDatum(tagId));
		
		if (!HeapTupleIsValid(desc_tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("object \"%s\" does not have tag \"%s\"",
			 		 objname, tagname)));

		/*
		 * Update existing tuple in pg_tag_description
		 */
		memset(tag_desc_repl_val, 0, sizeof(tag_desc_repl_val));
		memset(tag_desc_repl_null, false, sizeof(tag_desc_repl_null));
		memset(tag_desc_repl_repl, false, sizeof(tag_desc_repl_repl));

//		/* Extract the current tagvalue */
//		datum = SysCacheGetAttr(TAGDESCRIPTION,
//								desc_tuple,
//								Anum_pg_tag_description_tagvalue,
//								&isnull);
//
//		if (isnull)
//			datum = PointerGetDatum(NULL);

		/* Prepare the values array */
		datum = transformTagValues(TAG_VALUES_ACTION_ADD,
								   PointerGetDatum(NULL),
								   list_make1(makeString(tagvalue)));

		if (PointerIsValid(DatumGetPointer(datum)))
			tag_desc_repl_val[Anum_pg_tag_description_tagvalue - 1] = datum;
		else
			tag_desc_repl_null[Anum_pg_tag_description_tagvalue - 1] = true;

		tag_desc_repl_repl[Anum_pg_tag_description_tagvalue - 1] = true;

		/* Everything looks good - update the tuple */
		new_tuple = heap_modify_tuple(desc_tuple, RelationGetDescr(tag_desc_rel),
									  tag_desc_repl_val, tag_desc_repl_null, tag_desc_repl_repl);

		CatalogTupleUpdate(tag_desc_rel, &new_tuple->t_self, new_tuple);

		heap_freetuple(new_tuple);

		ReleaseSysCache(desc_tuple);
		ReleaseSysCache(tuple);
		
		CommandCounterIncrement();
	}

	table_close(tag_desc_rel, RowExclusiveLock);
	table_close(tag_rel, NoLock);
	
	return;
}


/*
 * Unset tags for object
 * 
 * Unset tag description value for object.
 */
void
UnsetTagDescriptions(List *tags,
					 Oid classid,
					 Oid objid,
					 int32 objsubid,
					 char *objname)
{
	Relation	tag_rel;
	Relation	tag_desc_rel;
	ListCell	*cell;

	tag_rel = table_open(TagRelationId, RowExclusiveLock);
	tag_desc_rel = table_open(TagDescriptionRelationId, RowExclusiveLock);
	
	foreach(cell, tags)
	{
		HeapTuple	tuple;
		HeapTuple	desc_tuple;
		HeapTuple	new_tuple;
		Form_pg_tag tagform;
		Oid		tagId;
		char	*tagname = NULL;
		Datum	tag_desc_repl_val[Natts_pg_tag_description];
		bool	tag_desc_repl_null[Natts_pg_tag_description];
		bool	tag_desc_repl_repl[Natts_pg_tag_description];
		
		tagname = strVal(lfirst(cell));
		
		tuple = SearchSysCache1(TAGNAME, CStringGetDatum(tagname));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for tag %s", tagname);

		tagform = (Form_pg_tag) GETSTRUCT(tuple);
		tagId = tagform->oid;

		desc_tuple = SearchSysCache4(TAGDESCRIPTION,
									 ObjectIdGetDatum(classid),
									 ObjectIdGetDatum(objid),
									 Int32GetDatum(objsubid),
									 ObjectIdGetDatum(tagId));

		if (!HeapTupleIsValid(desc_tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("object \"%s\" does not have tag \"%s\"",
					 objname, tagname)));

		/*
		 * Unset existing tag value in pg_tag_description
		 */
		memset(tag_desc_repl_val, 0, sizeof(tag_desc_repl_val));
		memset(tag_desc_repl_null, false, sizeof(tag_desc_repl_null));
		memset(tag_desc_repl_repl, false, sizeof(tag_desc_repl_repl));

		tag_desc_repl_null[Anum_pg_tag_description_tagvalue - 1] = true;

		/* Everything looks good - update the tuple */
		new_tuple = heap_modify_tuple(desc_tuple, RelationGetDescr(tag_desc_rel),
									  tag_desc_repl_val, tag_desc_repl_null, tag_desc_repl_repl);

		CatalogTupleUpdate(tag_desc_rel, &new_tuple->t_self, new_tuple);

		heap_freetuple(new_tuple);

		ReleaseSysCache(desc_tuple);
		ReleaseSysCache(tuple);

		CommandCounterIncrement();
	}

	table_close(tag_desc_rel, RowExclusiveLock);
	table_close(tag_rel, NoLock);

	return;
}

/*
 * Remove tag for object
 * 
 * Remove tag for database object such as database, warehouse, table etc.
 */
void
DeleteTagDescriptions(Oid classid,
					  Oid objid,
					  int32 objsubid,
					  const char *objname)
{
	Oid		tagdescId;
	HeapTuple	desc_tuple;
	Relation	rel;
	ScanKeyData	skey[3];
	SysScanDesc	scan;
	Form_pg_tag tagform;
	
	ScanKeyInit(&skey[0],
				Anum_pg_tag_description_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(classid));
	ScanKeyInit(&skey[1],
			 	Anum_pg_tag_description_objid,
			 	BTEqualStrategyNumber, F_OIDEQ,
			 	ObjectIdGetDatum(objid));
	ScanKeyInit(&skey[2],
			 	Anum_pg_tag_description_objsubid,
			 	BTEqualStrategyNumber, F_INT4EQ,
			 	Int32GetDatum(objsubid));

	rel = table_open(TagDescriptionRelationId, RowExclusiveLock);

	scan = systable_beginscan(rel, TagDescriptionIndexId, true,
							  NULL, 3, skey);
	
	while ((desc_tuple = systable_getnext(scan)) != NULL)
	{
		tagform = (Form_pg_tag) GETSTRUCT(desc_tuple);
		tagdescId = tagform->oid;
		
		CatalogTupleDelete(rel, &desc_tuple->t_self);

		/*
		 * Delete shared dependency references related to this tag description object.
		 */
		deleteSharedDependencyRecordsFor(TagDescriptionRelationId, tagdescId, 0);
	}

	systable_endscan(scan);

	/* Hold lock until transaction commit */
	table_close(rel, NoLock);
}


/*
 * Transform tag values to datum.
 */
static Datum
transformTagValues(int action, Datum oldvalues,
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

			/* Max allowed value length is 256. */
			if (strlen(value) > 256)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("added allowed value \"%s\" has exceeded max 256 length",
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

	/* Max allowed_values number is 300. */
	if (list_length(resultValues) > 300)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("Allowed_values only allow 300 values.")));
	
	result = valueListToArray(resultValues);
	
	return result;
}

/*
 * Untranform tag values from datum to list.
 */
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

/*
 * Get array datum from allowed_values list.
 */
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

char *
TagGetNameByOid(Oid tagid, bool missing_ok)
{
	char		*tagname;
	HeapTuple	tuple;
	Form_pg_tag tagform;
	
	tuple = SearchSysCache1(TAGOID, ObjectIdGetDatum(tagid));
	if (!HeapTupleIsValid(tuple))
	{
		if (!missing_ok)
			elog(ERROR, "tag %u could not be found", tagid);
		
		tagname = NULL;
	}
	else
	{
		tagform = (Form_pg_tag) GETSTRUCT(tuple);
		
		tagname = pstrdup(tagform->tagname.data);
	}
	ReleaseSysCache(tuple);
	
	return tagname;
}
