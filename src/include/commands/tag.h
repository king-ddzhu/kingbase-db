/*-------------------------------------------------------------------------
 *
 * tag.h
 *		Tag management commands (create/drop/alter tag).
 *
 *
 * Portions Copyright (c) 2024	Hashdata Inc
 *
 * src/include/commands/tag.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TAG_H
#define TAG_H
#include "catalog/objectaddress.h"

extern Oid CreateTag(CreateTagStmt *stmt);
extern ObjectAddress AlterTag(AlterTagStmt *stmt);
extern void DropTag(DropTagStmt *stmt);
extern void AddTagsForObject(List *tags, Oid classid, Oid objid, Oid objsubid, char *objname);
extern void AlterTagsForObject(List *tags, Oid classid, Oid objid, Oid objsubid, char *objname);
extern void UnsetTagsForObject(List *tags, Oid classid, Oid objid, Oid objsubid, char *objname);
extern Oid get_tag_oid(const char *tagname, bool missing_ok);
extern ObjectAddress RenameTag(const char *oldname, const char *newname);

#endif							/* TAG_H */