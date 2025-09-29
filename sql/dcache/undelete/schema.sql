CREATE TABLE public.t_deleted_paths (
    ipnfsid character varying(36)  CONSTRAINT t_deleted_files PRIMARY KEY,
    iname character varying(4096) NOT NULL,
    imode integer NOT NULL,
    iuid integer NOT NULL,
    igid integer NOT NULL,
    isize bigint NOT NULL,
    icrtime timestamp with time zone DEFAULT now() NOT NULL,
    iaccess_latency smallint
)
WITH (fillfactor='75', autovacuum_enabled='true');

CREATE OR REPLACE FUNCTION public.f_save_deleted_path() RETURNS trigger
       LANGUAGE plpgsql
                AS $$
                   BEGIN
                        IF (TG_OP = 'DELETE') THEN
                           IF EXISTS (SELECT 1 FROM t_locationinfo WHERE inumber = OLD.ichild and itype = 0)
                           THEN
                                INSERT INTO public.t_deleted_paths
                                       SELECT ipnfsid,
                                       inumber2path(inumber),
                                       imode,
                                       iuid,
                                       igid,
                                       isize,
                                       icrtime,
                                       iaccess_latency FROM t_inodes WHERE inumber = OLD.ichild and itype = 32768;
                           END IF;
                           RETURN OLD;
                        END IF;
                   END;
                   $$;


ALTER FUNCTION public.f_save_deleted_path() OWNER TO enstore;



CREATE TRIGGER tgr_save_deleted_path BEFORE DELETE ON public.t_dirs FOR EACH ROW EXECUTE FUNCTION public.f_save_deleted_path();
