--
-- Name: f_create_tag(character varying, character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_create_tag(character varying, character varying) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
       root varchar := $1;
       tag  varchar := $2;
       isorig integer;
       tagid varchar;
       pnfsid varchar;
       children RECORD;
begin
      SELECT INTO tagid itagid FROM t_tags
        WHERE ipnfsid = root AND itagname = tag;

       FOR pnfsid IN
           SELECT t_inodes.ipnfsid FROM t_inodes, t_dirs
           WHERE  t_inodes.itype=16384
           AND    t_dirs.iparent=root
           AND    t_inodes.ipnfsid=t_dirs.ipnfsid
           AND    t_dirs.iname NOT IN ('.', '..') loop
   begin
   INSERT INTO t_tags VALUES (pnfsid, tag, tagid, 0);
   end;
   PERFORM f_create_tag(pnfsid, tag);
       END loop;
end;
$_$;


ALTER FUNCTION public.f_create_tag(character varying, character varying) OWNER TO enstore;

--
-- Name: f_dir_size(character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_dir_size(character varying) RETURNS bigint
    LANGUAGE plpgsql
    AS $_$
declare
root varchar := $1;
ssum bigint  := 0;
children RECORD;
begin
for children in select t_inodes.ipnfsid, t_inodes.isize,t_inodes.itype
    from t_inodes, t_dirs
    where t_inodes.itype<>40960 and
          t_dirs.iparent=root and
  t_inodes.iio=0 and
  t_inodes.ipnfsid=t_dirs.ipnfsid and t_dirs.iname not in ('.', '..') loop
  IF children.itype = 32768 THEN
     ssum := ssum + children.isize;
  ELSE
     ssum := ssum + f_dir_size(children.ipnfsid);
  END IF;
  end loop;
  return ssum;
end;
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
IF NEW.ifiledata IS DISTINCT FROM OLD.ifiledata THEN
  data := trim( trailing E'\n' from encode(NEW.ifiledata,'escape'));
  select data||'c='||cs.itype||':'||cs.isum||';l='||ti.isize||';'||E'\n' into csum from t_inodes ti, t_inodes_checksum cs where ti.ipnfsid=cs.ipnfsid and ti.ipnfsid=NEW.ipnfsid;
  IF csum is NULL THEN
    RETURN NEW;
  END IF;
  length=character_length(csum);
  NEW.isize := length;
  NEW.ifiledata=decode(csum,'escape');
END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION public.f_enstore_add_to_level2() OWNER TO enstore;

--
-- Name: f_fill_al(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_fill_al() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  UPDATE t_inodes SET iaccess_latency = NEW.iaccesslatency WHERE ipnfsid = NEW.ipnfsid;
  RETURN NULL;
END;
$$;


ALTER FUNCTION public.f_fill_al() OWNER TO enstore;

--
-- Name: f_fill_rp(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_fill_rp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  UPDATE t_inodes SET iretention_policy = NEW.iretentionpolicy WHERE ipnfsid = NEW.ipnfsid;
  RETURN NULL;
END;
$$;


ALTER FUNCTION public.f_fill_rp() OWNER TO enstore;

--
-- Name: f_push_tag(); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_push_tag() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
           BEGIN
               PERFORM f_push_tag(NEW.ipnfsid, NEW.itagname);
           RETURN NULL;
           END;
           $$;


ALTER FUNCTION public.f_push_tag() OWNER TO enstore;

--
-- Name: f_push_tag(character varying, character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION f_push_tag(character varying, character varying) RETURNS void
    LANGUAGE plpgsql
    AS $_$
   DECLARE
   ---
   --- root is directory pnfsid
   --- tag name of the tag
   ---
       root varchar := $1;
       tag  varchar := $2;
       isorig integer;
       tagid varchar;
       pnfsid varchar;
   BEGIN
       SELECT INTO tagid itagid FROM t_tags
               WHERE ipnfsid = root AND itagname = tag;

       FOR pnfsid IN
           SELECT t_inodes.ipnfsid FROM t_inodes, t_dirs
           WHERE  t_inodes.itype=16384
           AND    t_dirs.iparent=root
           AND    t_inodes.ipnfsid=t_dirs.ipnfsid
           AND    t_dirs.iname NOT IN ('.', '..') loop

           BEGIN
                       SELECT INTO isorig isorign FROM t_tags
                       WHERE ipnfsid = pnfsid
                       AND itagname = tag;

               IF isorig = 0 THEN
                   DELETE FROM t_tags
                   WHERE ipnfsid = pnfsid
                   AND   itagname = tag;

                           INSERT INTO t_tags VALUES (pnfsid, tag, tagid, 0);
                           PERFORM f_push_tag(pnfsid, tag);
                       END IF;
           END;
       END loop;
   END;
   $_$;


ALTER FUNCTION public.f_push_tag(character varying, character varying) OWNER TO enstore;


--
-- Name: path2inode(character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION path2inode(path character varying) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
DECLARE
    elements varchar[] := string_to_array(trim(leading '/' from path), '/');
    child varchar;
    itype INT;
    link varchar;
    id varchar := '000000000000000000000000000000000000';
BEGIN
    FOR i IN 1..array_upper(elements,1) LOOP
        IF elements[i] = '""'  THEN
   CONTINUE;
        END IF;
        SELECT dir.ipnfsid, inode.itype INTO child, itype FROM t_dirs dir, t_inodes inode WHERE dir.ipnfsid = inode.ipnfsid AND dir.iparent=id AND dir.iname=elements[i];
        IF itype=40960 THEN
           SELECT encode(ifiledata,'escape') INTO link FROM t_inodes_data WHERE ipnfsid=child;
           IF link LIKE '/%' THEN
              child := path2inode('000000000000000000000000000000000000',
                                   substring(link from 2));
           ELSE
              child := path2inode(id, link);
           END IF;
        END IF;
        IF child IS NULL THEN
           RETURN NULL;
        END IF;
        id := child;
    END LOOP;
    RETURN id;
END;
$$;


ALTER FUNCTION public.path2inode(path character varying) OWNER TO enstore;



--
-- Name: push_tag(character varying, character varying); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION push_tag(character varying, character varying) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
root varchar := $1;
tag varchar := $2;
subdirs RECORD;
tagid varchar;
begin

select into tagid itagid from t_tags where ipnfsid = root and itagname = tag;

for subdirs in select t_inodes.ipnfsid, t_dirs.iname from t_inodes, t_dirs
where t_inodes.itype=16384 and t_dirs.iparent=root and
t_inodes.ipnfsid=t_dirs.ipnfsid and t_dirs.iname not in ('.', '..') loop
begin
delete from t_tags where ipnfsid = subdirs.ipnfsid and itagname = tag;
insert into t_tags values (subdirs.ipnfsid, tag, tagid, 0);
perform push_tag(subdirs.ipnfsid, tag);
end;
end loop;

end;
$_$;


ALTER FUNCTION public.push_tag(character varying, character varying) OWNER TO enstore;

--
-- Name: update_tag(character varying, character varying, character varying, integer); Type: FUNCTION; Schema: public; Owner: enstore
--

CREATE OR REPLACE FUNCTION update_tag(character varying, character varying, character varying, integer) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_pnfsid varchar := $1;
    v_tagname varchar := $2;
    v_tagid varchar := $3;
    v_isorigin int := $4;
BEGIN
   BEGIN
       INSERT INTO t_tags VALUES(v_pnfsid , v_tagname, v_tagid, v_isorigin);
   EXCEPTION WHEN unique_violation THEN
       RAISE NOTICE 'Tag % for % exist, updating.', v_tagname, v_pnfsid;
       UPDATE t_tags SET itagid=v_tagid,isorign=v_isorigin WHERE ipnfsid=v_pnfsid AND itagname=v_tagname;
   END;

END;

$_$;


ALTER FUNCTION public.update_tag(character varying, character varying, character varying, integer) OWNER TO enstore;

--
-- Name: bloat; Type: VIEW; Schema: public; Owner: enstore
--

CREATE VIEW bloat AS
 SELECT sml.schemaname,
    sml.tablename,
    (sml.reltuples)::bigint AS reltuples,
    (sml.relpages)::bigint AS relpages,
    sml.otta,
    round(
        CASE
            WHEN (sml.otta = (0)::double precision) THEN 0.0
            ELSE ((sml.relpages)::numeric / (sml.otta)::numeric)
        END, 1) AS tbloat,
    (((sml.relpages)::bigint)::double precision - sml.otta) AS wastedpages,
    (sml.bs * ((((sml.relpages)::double precision - sml.otta))::bigint)::numeric) AS wastedbytes,
    pg_size_pretty((((sml.bs)::double precision * ((sml.relpages)::double precision - sml.otta)))::bigint) AS wastedsize,
    sml.iname,
    (sml.ituples)::bigint AS ituples,
    (sml.ipages)::bigint AS ipages,
    sml.iotta,
    round(
        CASE
            WHEN ((sml.iotta = (0)::double precision) OR (sml.ipages = 0)) THEN 0.0
            ELSE ((sml.ipages)::numeric / (sml.iotta)::numeric)
        END, 1) AS ibloat,
        CASE
            WHEN ((sml.ipages)::double precision < sml.iotta) THEN (0)::double precision
            ELSE (((sml.ipages)::bigint)::double precision - sml.iotta)
        END AS wastedipages,
        CASE
            WHEN ((sml.ipages)::double precision < sml.iotta) THEN (0)::double precision
            ELSE ((sml.bs)::double precision * ((sml.ipages)::double precision - sml.iotta))
        END AS wastedibytes,
        CASE
            WHEN ((sml.ipages)::double precision < sml.iotta) THEN pg_size_pretty((0)::bigint)
            ELSE pg_size_pretty((((sml.bs)::double precision * ((sml.ipages)::double precision - sml.iotta)))::bigint)
        END AS wastedisize
   FROM ( SELECT rs.schemaname,
            rs.tablename,
            cc.reltuples,
            cc.relpages,
            rs.bs,
            ceil(((cc.reltuples * (((((rs.datahdr + (rs.ma)::numeric) -
                CASE
                    WHEN ((rs.datahdr % (rs.ma)::numeric) = (0)::numeric) THEN (rs.ma)::numeric
                    ELSE (rs.datahdr % (rs.ma)::numeric)
                END))::double precision + rs.nullhdr2) + (4)::double precision)) / ((rs.bs)::double precision - (20)::double precision))) AS otta,
            COALESCE(c2.relname, '?'::name) AS iname,
            COALESCE(c2.reltuples, (0)::real) AS ituples,
            COALESCE(c2.relpages, 0) AS ipages,
            COALESCE(ceil(((c2.reltuples * ((rs.datahdr - (12)::numeric))::double precision) / ((rs.bs)::double precision - (20)::double precision))), (0)::double precision) AS iotta
           FROM ((((( SELECT foo.ma,
                    foo.bs,
                    foo.schemaname,
                    foo.tablename,
                    ((foo.datawidth + (((foo.hdr + foo.ma) -
                        CASE
                            WHEN ((foo.hdr % foo.ma) = 0) THEN foo.ma
                            ELSE (foo.hdr % foo.ma)
                        END))::double precision))::numeric AS datahdr,
                    (foo.maxfracsum * (((foo.nullhdr + foo.ma) -
                        CASE
                            WHEN ((foo.nullhdr % (foo.ma)::bigint) = 0) THEN (foo.ma)::bigint
                            ELSE (foo.nullhdr % (foo.ma)::bigint)
                        END))::double precision) AS nullhdr2
                   FROM ( SELECT s.schemaname,
                            s.tablename,
                            constants.hdr,
                            constants.ma,
                            constants.bs,
                            sum((((1)::double precision - s.null_frac) * (s.avg_width)::double precision)) AS datawidth,
                            max(s.null_frac) AS maxfracsum,
                            (constants.hdr + ( SELECT (1 + (count(*) / 8))
                                   FROM pg_stats s2
                                  WHERE (((s2.null_frac <> (0)::double precision) AND (s2.schemaname = s.schemaname)) AND (s2.tablename = s.tablename)))) AS nullhdr
                           FROM pg_stats s,
                            ( SELECT ( SELECT (current_setting('block_size'::text))::numeric AS current_setting) AS bs,
CASE
 WHEN ("substring"(foo_1.v, 12, 3) = ANY (ARRAY['8.0'::text, '8.1'::text, '8.2'::text])) THEN 27
 ELSE 23
END AS hdr,
CASE
 WHEN (foo_1.v ~ 'mingw32'::text) THEN 8
 ELSE 4
END AS ma
                                   FROM ( SELECT version() AS v) foo_1) constants
                          GROUP BY s.schemaname, s.tablename, constants.hdr, constants.ma, constants.bs) foo) rs
             JOIN pg_class cc ON ((cc.relname = rs.tablename)))
             JOIN pg_namespace nn ON (((cc.relnamespace = nn.oid) AND (nn.nspname = rs.schemaname))))
             LEFT JOIN pg_index i ON ((i.indrelid = cc.oid)))
             LEFT JOIN pg_class c2 ON ((c2.oid = i.indexrelid)))) sml
  WHERE ((((sml.relpages)::double precision - sml.otta) > (0)::double precision) OR (((sml.ipages)::double precision - sml.iotta) > (10)::double precision))
  ORDER BY (sml.bs * ((((sml.relpages)::double precision - sml.otta))::bigint)::numeric) DESC,
        CASE
            WHEN ((sml.ipages)::double precision < sml.iotta) THEN (0)::double precision
            ELSE ((sml.bs)::double precision * ((sml.ipages)::double precision - sml.iotta))
        END DESC;


ALTER TABLE public.bloat OWNER TO enstore;

--
-- Name: orphans; Type: TABLE; Schema: public; Owner: enstore; Tablespace:
--

CREATE TABLE orphans (
    ipnfsid character varying(36)
);


ALTER TABLE public.orphans OWNER TO enstore;


--
-- Name: tgr_insert_al; Type: TRIGGER; Schema: public; Owner: enstore
--

CREATE TRIGGER tgr_insert_al AFTER INSERT OR UPDATE ON t_access_latency FOR EACH ROW EXECUTE PROCEDURE f_fill_al();


--
-- Name: tgr_insert_rp; Type: TRIGGER; Schema: public; Owner: enstore
--

CREATE TRIGGER tgr_insert_rp AFTER INSERT OR UPDATE ON t_retention_policy FOR EACH ROW EXECUTE PROCEDURE f_fill_rp();


--
-- Name: tgr_push_tag; Type: TRIGGER; Schema: public; Owner: enstore
--

CREATE TRIGGER tgr_push_tag AFTER UPDATE ON t_tags FOR EACH ROW EXECUTE PROCEDURE f_push_tag();


--
-- Name: tgr_enstore_level2; Type: TRIGGER; Schema: public; Owner: enstore
--

CREATE TRIGGER tgr_enstore_level2 BEFORE UPDATE ON t_level_2 FOR EACH ROW EXECUTE PROCEDURE f_enstore_add_to_level2();
