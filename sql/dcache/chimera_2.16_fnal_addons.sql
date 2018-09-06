--
-- Name: path2inode(character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION path2inumber(path character varying) RETURNS  BIGINT
    LANGUAGE plpgsql
    AS $$
DECLARE
    elements varchar[] := string_to_array(trim(leading '/' from path), '/');
    child varchar;
    itype INT;
    link varchar;
    type     int;
    id bigint := pnfsid2inumber('000000000000000000000000000000000000');
BEGIN
     IF array_length(elements, 1) is null THEN
        return id;
     END IF;
     FOR i IN 1..array_upper(elements,1) LOOP
         CASE
         WHEN elements[i] = '' THEN
             RETURN id;
         WHEN elements[i] = '.' THEN
             child := id;
         WHEN elements[i] = '..' THEN
             SELECT iparent INTO child FROM t_dirs WHERE ichild = id;
             IF NOT FOUND THEN
                 child := id;
             END IF;
         ELSE
             SELECT d.ichild, c.itype INTO child, type FROM t_dirs d JOIN t_inodes c ON d.ichild = c.inumber WHERE d.iparent = id AND d.iname = elements[i];
             IF type = 40960 THEN
                 SELECT encode(ifiledata,'escape') INTO link FROM t_inodes_data WHERE inumber = child;
                 IF link LIKE '/%' THEN
                     child := path2inumber(pnfsid2inumber('000000000000000000000000000000000000'),
                                                          substring(link from 2));
                 ELSE
                     child := path2inumber(id, link);
                 END IF;
             END IF;
         END CASE;
         IF child IS NULL THEN
             RETURN NULL;
         END IF;
         id := child;
     END LOOP;
     RETURN id;
END;
$$;

ALTER FUNCTION public.path2inumber(path character varying) OWNER TO enstore;

--
-- Name: path2inode(character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION path2inode(path character varying) RETURNS character varying
    LANGUAGE sql
    AS $_$
    SELECT inumber2pnfsid(path2inumber($1));
    $_$;

ALTER FUNCTION public.path2inode(path character varying) OWNER TO enstore;


--
-- Name: f_push_tag(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_push_tag() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
           BEGIN
               PERFORM f_push_tag(NEW.inumber, NEW.itagname);
           RETURN NULL;
           END;
           $$;


ALTER FUNCTION public.f_push_tag() OWNER TO enstore;

--
-- Name: f_push_tag(character varying, character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_push_tag(root bigint, tag character varying) RETURNS void
    LANGUAGE plpgsql
    AS $_$
   DECLARE
   ---
   --- root is directory inumber
   --- tag name of the tag
   ---
       isorig integer;
       tagid bigint;
       node bigint;
   BEGIN
       SELECT INTO tagid itagid FROM t_tags
               WHERE inumber = root AND itagname = tag;

       FOR node IN
           SELECT t_inodes.inumber FROM t_inodes, t_dirs
           WHERE  t_inodes.itype=16384
           AND    t_dirs.iparent=root
           AND    t_inodes.inumber=t_dirs.ichild
           AND    t_dirs.iname NOT IN ('.', '..') loop

           BEGIN
               SELECT INTO isorig isorign FROM t_tags
               WHERE inumber = node
               AND itagname = tag;

               IF isorig = 0 THEN
                   DELETE FROM t_tags
                   WHERE inumber = node
                   AND   itagname = tag;
                           INSERT INTO t_tags (inumber, itagname, itagid, isorign) VALUES (node, tag, tagid, 0);
                           PERFORM f_push_tag(node, tag);
               END IF;
           END;
       END loop;
   END;
   $_$;


ALTER FUNCTION public.f_push_tag(bigint, character varying) OWNER TO enstore;

DROP TRIGGER IF EXISTS tgr_push_tag ON t_tags;

CREATE TRIGGER tgr_push_tag AFTER UPDATE ON t_tags FOR EACH ROW EXECUTE PROCEDURE f_push_tag();

--
-- Name: f_create_tag(character varying, character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_create_tag(rootid bigint, tag character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
       isorig integer;
       tagid bigint;
       node bigint;
       children RECORD;
BEGIN
      SELECT INTO tagid itagid FROM t_tags
        WHERE inumber = rootid AND itagname = tag;

       FOR node IN
           SELECT t_inodes.inumber FROM t_inodes, t_dirs
           WHERE  t_inodes.itype=16384
           AND    t_dirs.iparent=rootid
           AND    t_inodes.inumber=t_dirs.ichild
           AND    t_dirs.iname NOT IN ('.', '..') loop
           BEGIN
	       INSERT INTO t_tags (inumber, itagname, itagid, isorign) VALUES (node, tag, tagid, 0);
	       EXCEPTION WHEN unique_violation THEN
	       --- do nothing ---
	       --- RAISE NOTICE 'Already exist % %',  inumber, itagname;
	       CONTINUE;
	   END;
	      PERFORM f_create_tag(node, tag);
       END loop;
END;
$$;

ALTER FUNCTION public.f_create_tag(bigint, character varying) OWNER TO enstore;

--
-- Name: f_create_tag(character varying, character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_delete_tag(rootid bigint, tag character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
       isorig integer;
       tagid bigint;
       node bigint;
       children RECORD;
BEGIN

      DELETE FROM t_tags
      WHERE inumber = rootid
      AND   itagname = tag;

       FOR node IN
           SELECT t_inodes.inumber FROM t_inodes, t_dirs
           WHERE  t_inodes.itype=16384
           AND    t_dirs.iparent=rootid
           AND    t_inodes.inumber=t_dirs.ichild
           AND    t_dirs.iname NOT IN ('.', '..') loop
           BEGIN
	       DELETE FROM t_tags
                   WHERE inumber = node
                   AND   itagname = tag;
	      PERFORM f_delete_tag(node, tag);
	   END;
       END loop;
END;
$$;

ALTER FUNCTION public.f_delete_tag(bigint, character varying) OWNER TO enstore;

--
-- Name: f_dir_size(character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_dir_size(root character varying) RETURNS bigint
    LANGUAGE plpgsql
    AS $_$
DECLARE
rootid bigint := pnfsid2inumber(root);
ssum bigint  := 0;
children RECORD;
BEGIN
    FOR children in SELECT t_inodes.ipnfsid, t_inodes.isize, t_inodes.itype
                         FROM t_inodes, t_dirs
			 WHERE t_inodes.itype<>40960 AND
                               t_dirs.iparent=rootid AND
                               t_inodes.iio=0 AND
                               t_inodes.inumber=t_dirs.ichild AND
			       t_dirs.iname NOT IN ('.', '..') loop
       IF children.itype = 32768 THEN
          ssum := ssum + children.isize;
       ELSE
          ssum := ssum + f_dir_size(children.ipnfsid);
       END IF;
     END loop;
     RETURN ssum;
END;
$_$;

ALTER FUNCTION public.f_dir_size(character varying) OWNER TO enstore;

--
-- Name: f_enstore_add_to_level2(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_enstore_add_to_level2() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  data character varying;
  csum character varying;
  length integer;
BEGIN
--- IF NEW.ifiledata IS DISTINCT FROM OLD.ifiledata THEN
  data := trim( trailing E'\n' from encode(NEW.ifiledata,'escape'));
  select data||'c='||cs.itype||':'||cs.isum||';l='||ti.isize||';'||E'\n' into csum from t_inodes ti, t_inodes_checksum cs where ti.inumber=cs.inumber and ti.inumber=NEW.inumber;
  IF csum is NULL THEN
    RETURN NEW;
  END IF;
  length=character_length(csum);
  NEW.isize := length;
  NEW.ifiledata=decode(csum,'escape');
--- END IF;
  RETURN NEW;
END;
$$;

ALTER FUNCTION public.f_enstore_add_to_level2() OWNER TO enstore;

DROP TRIGGER IF EXISTS tgr_enstore_level2 ON t_level_2;

-- CREATE TRIGGER tgr_enstore_level2 BEFORE UPDATE ON t_level_2 FOR EACH ROW EXECUTE PROCEDURE f_enstore_add_to_level2();
CREATE TRIGGER tgr_enstore_level2 BEFORE INSERT ON t_level_2 FOR EACH ROW EXECUTE PROCEDURE f_enstore_add_to_level2();