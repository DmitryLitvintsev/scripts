DROP TRIGGER IF EXISTS tgr_push_tag ON t_tags;
DROP FUNCTION f_push_tag(character varying, character varying);
DROP FUNCTION f_push_tag();

DROP TRIGGER IF EXISTS tgr_enstore_level2 ON t_level_2 ;
