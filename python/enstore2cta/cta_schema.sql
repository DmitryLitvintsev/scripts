--
-- PostgreSQL database dump
--

-- Dumped from database version 14.4
-- Dumped by pg_dump version 14.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: admin_user; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.admin_user (
    admin_user_name character varying(100) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.admin_user OWNER TO cta;

--
-- Name: archive_file; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.archive_file (
    archive_file_id numeric(20,0) NOT NULL,
    disk_instance_name character varying(100) NOT NULL,
    disk_file_id character varying(100) NOT NULL,
    disk_file_uid numeric(10,0) NOT NULL,
    disk_file_gid numeric(10,0) NOT NULL,
    size_in_bytes numeric(20,0) NOT NULL,
    checksum_blob bytea,
    checksum_adler32 numeric(10,0) NOT NULL,
    storage_class_id numeric(20,0) NOT NULL,
    creation_time numeric(20,0) NOT NULL,
    reconciliation_time numeric(20,0) NOT NULL,
    is_deleted character(1) DEFAULT '0'::bpchar NOT NULL,
    collocation_hint character varying(100),
    CONSTRAINT archive_file_id_bool_ck CHECK ((is_deleted = ANY (ARRAY['0'::bpchar, '1'::bpchar])))
);


ALTER TABLE public.archive_file OWNER TO cta;

--
-- Name: archive_file_id_seq; Type: SEQUENCE; Schema: public; Owner: cta
--

CREATE SEQUENCE public.archive_file_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


ALTER TABLE public.archive_file_id_seq OWNER TO cta;

--
-- Name: archive_route; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.archive_route (
    storage_class_id numeric(20,0) NOT NULL,
    copy_nb numeric(3,0) NOT NULL,
    tape_pool_id numeric(20,0) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL,
    CONSTRAINT archive_route_copy_nb_gt_0_ck CHECK ((copy_nb > (0)::numeric))
);


ALTER TABLE public.archive_route OWNER TO cta;

--
-- Name: cta_catalogue; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.cta_catalogue (
    schema_version_major numeric(20,0) NOT NULL,
    schema_version_minor numeric(20,0) NOT NULL,
    next_schema_version_major numeric(20,0),
    next_schema_version_minor numeric(20,0),
    status character varying(100),
    is_production character(1) DEFAULT '0'::bpchar NOT NULL,
    CONSTRAINT catalogue_status_content_ck CHECK ((((next_schema_version_major IS NULL) AND (next_schema_version_minor IS NULL) AND ((status)::text = 'PRODUCTION'::text)) OR ((status)::text = 'UPGRADING'::text))),
    CONSTRAINT cta_catalogue_ip_bool_ck CHECK ((is_production = ANY (ARRAY['0'::bpchar, '1'::bpchar])))
);


ALTER TABLE public.cta_catalogue OWNER TO cta;

--
-- Name: databasechangelog; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.databasechangelog (
    id character varying(255) NOT NULL,
    author character varying(255) NOT NULL,
    filename character varying(255) NOT NULL,
    dateexecuted timestamp with time zone NOT NULL,
    orderexecuted integer NOT NULL,
    exectype character varying(10) NOT NULL,
    md5sum character varying(35),
    description character varying(255),
    comments character varying(255),
    tag character varying(255),
    liquibase character varying(20)
);


ALTER TABLE public.databasechangelog OWNER TO cta;

--
-- Name: databasechangeloglock; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.databasechangeloglock (
    id integer NOT NULL,
    locked boolean NOT NULL,
    lockgranted timestamp with time zone,
    lockedby character varying(255)
);


ALTER TABLE public.databasechangeloglock OWNER TO cta;

--
-- Name: disk_instance; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.disk_instance (
    disk_instance_name character varying(100) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.disk_instance OWNER TO cta;

--
-- Name: disk_instance_space; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.disk_instance_space (
    disk_instance_name character varying(100) NOT NULL,
    disk_instance_space_name character varying(100) NOT NULL,
    free_space_query_url character varying(1000) NOT NULL,
    refresh_interval numeric(20,0) NOT NULL,
    last_refresh_time numeric(20,0) NOT NULL,
    free_space numeric(20,0) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.disk_instance_space OWNER TO cta;

--
-- Name: disk_system; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.disk_system (
    disk_system_name character varying(100) NOT NULL,
    disk_instance_name character varying(100) NOT NULL,
    disk_instance_space_name character varying(100) NOT NULL,
    file_regexp character varying(100) NOT NULL,
    targeted_free_space numeric(20,0) NOT NULL,
    sleep_time numeric(20,0) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.disk_system OWNER TO cta;

--
-- Name: drive_config; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.drive_config (
    drive_name character varying(100) NOT NULL,
    category character varying(100) NOT NULL,
    key_name character varying(100) NOT NULL,
    value character varying(1000) NOT NULL,
    source character varying(100) NOT NULL
);


ALTER TABLE public.drive_config OWNER TO cta;

--
-- Name: drive_state; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.drive_state (
    drive_name character varying(100) NOT NULL,
    host character varying(100) NOT NULL,
    logical_library character varying(100) NOT NULL,
    session_id numeric(20,0),
    bytes_transfered_in_session numeric(20,0),
    files_transfered_in_session numeric(20,0),
    session_start_time numeric(20,0),
    session_elapsed_time numeric(20,0),
    mount_start_time numeric(20,0),
    transfer_start_time numeric(20,0),
    unload_start_time numeric(20,0),
    unmount_start_time numeric(20,0),
    draining_start_time numeric(20,0),
    down_or_up_start_time numeric(20,0),
    probe_start_time numeric(20,0),
    cleanup_start_time numeric(20,0),
    start_start_time numeric(20,0),
    shutdown_time numeric(20,0),
    mount_type character varying(100) DEFAULT 'NO_MOUNT'::character varying NOT NULL,
    drive_status character varying(100) DEFAULT 'UNKNOWN'::character varying NOT NULL,
    desired_up character(1) DEFAULT '0'::bpchar NOT NULL,
    desired_force_down character(1) DEFAULT '0'::bpchar NOT NULL,
    reason_up_down character varying(1000),
    current_vid character varying(100),
    cta_version character varying(100),
    current_priority numeric(20,0),
    current_activity character varying(100),
    current_tape_pool character varying(100),
    next_mount_type character varying(100) DEFAULT 'NO_MOUNT'::character varying NOT NULL,
    next_vid character varying(100),
    next_priority numeric(20,0),
    next_activity character varying(100),
    next_tape_pool character varying(100),
    dev_file_name character varying(100),
    raw_library_slot character varying(100),
    current_vo character varying(100),
    next_vo character varying(100),
    user_comment character varying(1000),
    creation_log_user_name character varying(100),
    creation_log_host_name character varying(100),
    creation_log_time numeric(20,0),
    last_update_user_name character varying(100),
    last_update_host_name character varying(100),
    last_update_time numeric(20,0),
    disk_system_name character varying(100),
    reserved_bytes numeric(20,0),
    reservation_session_id numeric(20,0),
    CONSTRAINT drive_dfd_bool_ck CHECK ((desired_force_down = ANY (ARRAY['0'::bpchar, '1'::bpchar]))),
    CONSTRAINT drive_ds_string_ck CHECK (((drive_status)::text = ANY (ARRAY[('DOWN'::character varying)::text, ('UP'::character varying)::text, ('PROBING'::character varying)::text, ('STARTING'::character varying)::text, ('MOUNTING'::character varying)::text, ('TRANSFERING'::character varying)::text, ('UNLOADING'::character varying)::text, ('UNMOUNTING'::character varying)::text, ('DRAININGTODISK'::character varying)::text, ('CLEANINGUP'::character varying)::text, ('SHUTDOWN'::character varying)::text, ('UNKNOWN'::character varying)::text]))),
    CONSTRAINT drive_du_bool_ck CHECK ((desired_up = ANY (ARRAY['0'::bpchar, '1'::bpchar]))),
    CONSTRAINT drive_mt_string_ck CHECK (((mount_type)::text = ANY (ARRAY[('NO_MOUNT'::character varying)::text, ('ARCHIVE_FOR_USER'::character varying)::text, ('ARCHIVE_FOR_REPACK'::character varying)::text, ('RETRIEVE'::character varying)::text, ('LABEL'::character varying)::text, ('ARCHIVE_ALL_TYPES'::character varying)::text]))),
    CONSTRAINT drive_nmt_string_ck CHECK (((next_mount_type)::text = ANY (ARRAY[('NO_MOUNT'::character varying)::text, ('ARCHIVE_FOR_USER'::character varying)::text, ('ARCHIVE_FOR_REPACK'::character varying)::text, ('RETRIEVE'::character varying)::text, ('LABEL'::character varying)::text, ('ARCHIVE_ALL_TYPES'::character varying)::text])))
);


ALTER TABLE public.drive_state OWNER TO cta;

--
-- Name: file_recycle_log; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.file_recycle_log (
    file_recycle_log_id numeric(20,0) NOT NULL,
    vid character varying(100) NOT NULL,
    fseq numeric(20,0) NOT NULL,
    block_id numeric(20,0) NOT NULL,
    copy_nb numeric(3,0) NOT NULL,
    tape_file_creation_time numeric(20,0) NOT NULL,
    archive_file_id numeric(20,0) NOT NULL,
    disk_instance_name character varying(100) NOT NULL,
    disk_file_id character varying(100) NOT NULL,
    disk_file_id_when_deleted character varying(100) NOT NULL,
    disk_file_uid numeric(20,0) NOT NULL,
    disk_file_gid numeric(20,0) NOT NULL,
    size_in_bytes numeric(20,0) NOT NULL,
    checksum_blob bytea,
    checksum_adler32 numeric(10,0) NOT NULL,
    storage_class_id numeric(20,0) NOT NULL,
    archive_file_creation_time numeric(20,0) NOT NULL,
    reconciliation_time numeric(20,0) NOT NULL,
    collocation_hint character varying(100),
    disk_file_path character varying(2000),
    reason_log character varying(1000) NOT NULL,
    recycle_log_time numeric(20,0) NOT NULL
);


ALTER TABLE public.file_recycle_log OWNER TO cta;

--
-- Name: file_recycle_log_id_seq; Type: SEQUENCE; Schema: public; Owner: cta
--

CREATE SEQUENCE public.file_recycle_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


ALTER TABLE public.file_recycle_log_id_seq OWNER TO cta;

--
-- Name: logical_library; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.logical_library (
    logical_library_id numeric(20,0) NOT NULL,
    logical_library_name character varying(100) NOT NULL,
    is_disabled character(1) DEFAULT '0'::bpchar NOT NULL,
    disabled_reason character varying(1000),
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL,
    CONSTRAINT logical_library_id_bool_ck CHECK ((is_disabled = ANY (ARRAY['0'::bpchar, '1'::bpchar])))
);


ALTER TABLE public.logical_library OWNER TO cta;

--
-- Name: logical_library_id_seq; Type: SEQUENCE; Schema: public; Owner: cta
--

CREATE SEQUENCE public.logical_library_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


ALTER TABLE public.logical_library_id_seq OWNER TO cta;

--
-- Name: media_type; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.media_type (
    media_type_id numeric(20,0) NOT NULL,
    media_type_name character varying(100) NOT NULL,
    cartridge character varying(100) NOT NULL,
    capacity_in_bytes numeric(20,0) NOT NULL,
    primary_density_code numeric(3,0),
    secondary_density_code numeric(3,0),
    nb_wraps numeric(10,0),
    min_lpos numeric(20,0),
    max_lpos numeric(20,0),
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.media_type OWNER TO cta;

--
-- Name: media_type_id_seq; Type: SEQUENCE; Schema: public; Owner: cta
--

CREATE SEQUENCE public.media_type_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


ALTER TABLE public.media_type_id_seq OWNER TO cta;

--
-- Name: mount_policy; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.mount_policy (
    mount_policy_name character varying(100) NOT NULL,
    archive_priority numeric(20,0) NOT NULL,
    archive_min_request_age numeric(20,0) NOT NULL,
    retrieve_priority numeric(20,0) NOT NULL,
    retrieve_min_request_age numeric(20,0) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.mount_policy OWNER TO cta;

--
-- Name: requester_activity_mount_rule; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.requester_activity_mount_rule (
    disk_instance_name character varying(100) NOT NULL,
    requester_name character varying(100) NOT NULL,
    activity_regex character varying(100) NOT NULL,
    mount_policy_name character varying(100) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.requester_activity_mount_rule OWNER TO cta;

--
-- Name: requester_group_mount_rule; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.requester_group_mount_rule (
    disk_instance_name character varying(100) NOT NULL,
    requester_group_name character varying(100) NOT NULL,
    mount_policy_name character varying(100) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.requester_group_mount_rule OWNER TO cta;

--
-- Name: requester_mount_rule; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.requester_mount_rule (
    disk_instance_name character varying(100) NOT NULL,
    requester_name character varying(100) NOT NULL,
    mount_policy_name character varying(100) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.requester_mount_rule OWNER TO cta;

--
-- Name: storage_class; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.storage_class (
    storage_class_id numeric(20,0) NOT NULL,
    storage_class_name character varying(100) NOT NULL,
    nb_copies numeric(3,0) NOT NULL,
    virtual_organization_id numeric(20,0) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL
);


ALTER TABLE public.storage_class OWNER TO cta;

--
-- Name: storage_class_id_seq; Type: SEQUENCE; Schema: public; Owner: cta
--

CREATE SEQUENCE public.storage_class_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


ALTER TABLE public.storage_class_id_seq OWNER TO cta;

--
-- Name: tape; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.tape (
    vid character varying(100) NOT NULL,
    media_type_id numeric(20,0) NOT NULL,
    vendor character varying(100) NOT NULL,
    logical_library_id numeric(20,0) NOT NULL,
    tape_pool_id numeric(20,0) NOT NULL,
    encryption_key_name character varying(100),
    data_in_bytes numeric(20,0) NOT NULL,
    last_fseq numeric(20,0) NOT NULL,
    nb_master_files numeric(20,0) DEFAULT 0 NOT NULL,
    master_data_in_bytes numeric(20,0) DEFAULT 0 NOT NULL,
    is_full character(1) NOT NULL,
    is_from_castor character(1) NOT NULL,
    dirty character(1) DEFAULT '1'::bpchar NOT NULL,
    nb_copy_nb_1 numeric(20,0) DEFAULT 0 NOT NULL,
    copy_nb_1_in_bytes numeric(20,0) DEFAULT 0 NOT NULL,
    nb_copy_nb_gt_1 numeric(20,0) DEFAULT 0 NOT NULL,
    copy_nb_gt_1_in_bytes numeric(20,0) DEFAULT 0 NOT NULL,
    label_format character(1),
    label_drive character varying(100),
    label_time numeric(20,0),
    last_read_drive character varying(100),
    last_read_time numeric(20,0),
    last_write_drive character varying(100),
    last_write_time numeric(20,0),
    read_mount_count numeric(20,0) DEFAULT 0 NOT NULL,
    write_mount_count numeric(20,0) DEFAULT 0 NOT NULL,
    user_comment character varying(1000),
    tape_state character varying(100) NOT NULL,
    state_reason character varying(1000),
    state_update_time numeric(20,0) NOT NULL,
    state_modified_by character varying(100) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL,
    verification_status character varying(1000),
    CONSTRAINT tape_dirty_bool_ck CHECK ((dirty = ANY (ARRAY['0'::bpchar, '1'::bpchar]))),
    CONSTRAINT tape_is_from_castor_bool_ck CHECK ((is_from_castor = ANY (ARRAY['0'::bpchar, '1'::bpchar]))),
    CONSTRAINT tape_is_full_bool_ck CHECK ((is_full = ANY (ARRAY['0'::bpchar, '1'::bpchar]))),
    CONSTRAINT tape_state_ck CHECK (((tape_state)::text = ANY ((ARRAY['ACTIVE'::character varying, 'REPACKING_PENDING'::character varying, 'REPACKING'::character varying, 'REPACKING_DISABLED'::character varying, 'DISABLED'::character varying, 'BROKEN_PENDING'::character varying, 'BROKEN'::character varying, 'EXPORTED_PENDING'::character varying, 'EXPORTED'::character varying])::text[])))
);


ALTER TABLE public.tape OWNER TO cta;

--
-- Name: tape_file; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.tape_file (
    vid character varying(100) NOT NULL,
    fseq numeric(20,0) NOT NULL,
    block_id numeric(20,0) NOT NULL,
    logical_size_in_bytes numeric(20,0) NOT NULL,
    copy_nb numeric(3,0) NOT NULL,
    creation_time numeric(20,0) NOT NULL,
    archive_file_id numeric(20,0) NOT NULL,
    CONSTRAINT tape_file_copy_nb_gt_0_ck CHECK ((copy_nb > (0)::numeric))
);


ALTER TABLE public.tape_file OWNER TO cta;

--
-- Name: tape_pool; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.tape_pool (
    tape_pool_id numeric(20,0) NOT NULL,
    tape_pool_name character varying(100) NOT NULL,
    virtual_organization_id numeric(20,0) NOT NULL,
    nb_partial_tapes numeric(20,0) NOT NULL,
    is_encrypted character(1) NOT NULL,
    supply character varying(100),
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL,
    encryption_key_name character varying(100),
    CONSTRAINT tape_pool_is_encrypted_bool_ck CHECK ((is_encrypted = ANY (ARRAY['0'::bpchar, '1'::bpchar])))
);


ALTER TABLE public.tape_pool OWNER TO cta;

--
-- Name: tape_pool_id_seq; Type: SEQUENCE; Schema: public; Owner: cta
--

CREATE SEQUENCE public.tape_pool_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


ALTER TABLE public.tape_pool_id_seq OWNER TO cta;

--
-- Name: virtual_organization; Type: TABLE; Schema: public; Owner: cta
--

CREATE TABLE public.virtual_organization (
    virtual_organization_id numeric(20,0) NOT NULL,
    virtual_organization_name character varying(100) NOT NULL,
    read_max_drives numeric(20,0) NOT NULL,
    write_max_drives numeric(20,0) NOT NULL,
    max_file_size numeric(20,0) NOT NULL,
    user_comment character varying(1000) NOT NULL,
    creation_log_user_name character varying(100) NOT NULL,
    creation_log_host_name character varying(100) NOT NULL,
    creation_log_time numeric(20,0) NOT NULL,
    last_update_user_name character varying(100) NOT NULL,
    last_update_host_name character varying(100) NOT NULL,
    last_update_time numeric(20,0) NOT NULL,
    disk_instance_name character varying(100) NOT NULL
);


ALTER TABLE public.virtual_organization OWNER TO cta;

--
-- Name: virtual_organization_id_seq; Type: SEQUENCE; Schema: public; Owner: cta
--

CREATE SEQUENCE public.virtual_organization_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


ALTER TABLE public.virtual_organization_id_seq OWNER TO cta;

--
-- Name: admin_user admin_user_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.admin_user
    ADD CONSTRAINT admin_user_pk PRIMARY KEY (admin_user_name);


--
-- Name: archive_file archive_file_din_dfi_un; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.archive_file
    ADD CONSTRAINT archive_file_din_dfi_un UNIQUE (disk_instance_name, disk_file_id) DEFERRABLE;


--
-- Name: archive_file archive_file_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.archive_file
    ADD CONSTRAINT archive_file_pk PRIMARY KEY (archive_file_id);


--
-- Name: archive_route archive_route_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.archive_route
    ADD CONSTRAINT archive_route_pk PRIMARY KEY (storage_class_id, copy_nb);


--
-- Name: archive_route archive_route_sci_tpi_un; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.archive_route
    ADD CONSTRAINT archive_route_sci_tpi_un UNIQUE (storage_class_id, tape_pool_id);


--
-- Name: disk_instance disk_instance_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.disk_instance
    ADD CONSTRAINT disk_instance_pk PRIMARY KEY (disk_instance_name);


--
-- Name: disk_instance_space disk_instance_space_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.disk_instance_space
    ADD CONSTRAINT disk_instance_space_pk PRIMARY KEY (disk_instance_name, disk_instance_space_name);


--
-- Name: drive_config drive_config_dn_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.drive_config
    ADD CONSTRAINT drive_config_dn_pk PRIMARY KEY (key_name, drive_name);


--
-- Name: drive_state drive_dn_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.drive_state
    ADD CONSTRAINT drive_dn_pk PRIMARY KEY (drive_name);


--
-- Name: file_recycle_log file_recycle_log_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.file_recycle_log
    ADD CONSTRAINT file_recycle_log_pk PRIMARY KEY (file_recycle_log_id);


--
-- Name: logical_library logical_library_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.logical_library
    ADD CONSTRAINT logical_library_pk PRIMARY KEY (logical_library_id);


--
-- Name: media_type media_type_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.media_type
    ADD CONSTRAINT media_type_pk PRIMARY KEY (media_type_id);


--
-- Name: mount_policy mount_policy_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.mount_policy
    ADD CONSTRAINT mount_policy_pk PRIMARY KEY (mount_policy_name);


--
-- Name: disk_system name_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.disk_system
    ADD CONSTRAINT name_pk PRIMARY KEY (disk_system_name);


--
-- Name: databasechangeloglock pk_databasechangeloglock; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.databasechangeloglock
    ADD CONSTRAINT pk_databasechangeloglock PRIMARY KEY (id);


--
-- Name: requester_activity_mount_rule rqster_act_rule_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_activity_mount_rule
    ADD CONSTRAINT rqster_act_rule_pk PRIMARY KEY (disk_instance_name, requester_name, activity_regex);


--
-- Name: requester_group_mount_rule rqster_grp_rule_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_group_mount_rule
    ADD CONSTRAINT rqster_grp_rule_pk PRIMARY KEY (disk_instance_name, requester_group_name);


--
-- Name: requester_mount_rule rqster_rule_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_mount_rule
    ADD CONSTRAINT rqster_rule_pk PRIMARY KEY (disk_instance_name, requester_name);


--
-- Name: storage_class storage_class_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.storage_class
    ADD CONSTRAINT storage_class_pk PRIMARY KEY (storage_class_id);


--
-- Name: tape_file tape_file_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape_file
    ADD CONSTRAINT tape_file_pk PRIMARY KEY (vid, fseq);


--
-- Name: tape_file tape_file_vid_block_id_un; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape_file
    ADD CONSTRAINT tape_file_vid_block_id_un UNIQUE (vid, block_id);


--
-- Name: tape tape_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape
    ADD CONSTRAINT tape_pk PRIMARY KEY (vid);


--
-- Name: tape_pool tape_pool_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape_pool
    ADD CONSTRAINT tape_pool_pk PRIMARY KEY (tape_pool_id);


--
-- Name: virtual_organization virtual_organization_pk; Type: CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.virtual_organization
    ADD CONSTRAINT virtual_organization_pk PRIMARY KEY (virtual_organization_id);


--
-- Name: admin_user_aun_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX admin_user_aun_ci_un_idx ON public.admin_user USING btree (lower((admin_user_name)::text));


--
-- Name: archive_file_dfi_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX archive_file_dfi_idx ON public.archive_file USING btree (disk_file_id);


--
-- Name: archive_file_din_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX archive_file_din_idx ON public.archive_file USING btree (disk_instance_name);


--
-- Name: archive_file_sci_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX archive_file_sci_idx ON public.archive_file USING btree (storage_class_id);


--
-- Name: disk_instance_din_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX disk_instance_din_ci_un_idx ON public.disk_instance USING btree (lower((disk_instance_name)::text));


--
-- Name: disk_instance_space_disn_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX disk_instance_space_disn_ci_un_idx ON public.disk_instance_space USING btree (lower((disk_instance_space_name)::text));


--
-- Name: disk_instance_space_disn_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX disk_instance_space_disn_un_idx ON public.disk_instance_space USING btree (disk_instance_space_name);


--
-- Name: disk_system_din_disn_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX disk_system_din_disn_idx ON public.disk_system USING btree (disk_instance_name, disk_instance_space_name);


--
-- Name: disk_system_dsn_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX disk_system_dsn_ci_un_idx ON public.disk_system USING btree (lower((disk_system_name)::text));


--
-- Name: drive_state_dn_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX drive_state_dn_ci_un_idx ON public.drive_state USING btree (lower((drive_name)::text));


--
-- Name: file_recycle_log_dfi_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX file_recycle_log_dfi_idx ON public.file_recycle_log USING btree (disk_file_id);


--
-- Name: file_recycle_log_scd_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX file_recycle_log_scd_idx ON public.file_recycle_log USING btree (storage_class_id);


--
-- Name: file_recycle_log_vid_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX file_recycle_log_vid_idx ON public.file_recycle_log USING btree (vid);


--
-- Name: logical_library_lln_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX logical_library_lln_ci_un_idx ON public.logical_library USING btree (lower((logical_library_name)::text));


--
-- Name: logical_library_lln_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX logical_library_lln_un_idx ON public.logical_library USING btree (logical_library_name);


--
-- Name: media_type_mtn_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX media_type_mtn_ci_un_idx ON public.media_type USING btree (lower((media_type_name)::text));


--
-- Name: media_type_mtn_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX media_type_mtn_un_idx ON public.media_type USING btree (media_type_name);


--
-- Name: mount_policy_mpn_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX mount_policy_mpn_ci_un_idx ON public.mount_policy USING btree (lower((mount_policy_name)::text));


--
-- Name: req_act_mnt_rule_din_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX req_act_mnt_rule_din_idx ON public.requester_activity_mount_rule USING btree (disk_instance_name);


--
-- Name: req_act_mnt_rule_mpn_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX req_act_mnt_rule_mpn_idx ON public.requester_activity_mount_rule USING btree (mount_policy_name);


--
-- Name: req_grp_mnt_rule_din_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX req_grp_mnt_rule_din_idx ON public.requester_group_mount_rule USING btree (disk_instance_name);


--
-- Name: req_grp_mnt_rule_mpn_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX req_grp_mnt_rule_mpn_idx ON public.requester_group_mount_rule USING btree (mount_policy_name);


--
-- Name: req_mnt_rule_din_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX req_mnt_rule_din_idx ON public.requester_mount_rule USING btree (disk_instance_name);


--
-- Name: req_mnt_rule_mpn_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX req_mnt_rule_mpn_idx ON public.requester_mount_rule USING btree (mount_policy_name);


--
-- Name: storage_class_scn_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX storage_class_scn_ci_un_idx ON public.storage_class USING btree (lower((storage_class_name)::text));


--
-- Name: storage_class_scn_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX storage_class_scn_un_idx ON public.storage_class USING btree (storage_class_name);


--
-- Name: storage_class_voi_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX storage_class_voi_idx ON public.storage_class USING btree (virtual_organization_id);


--
-- Name: tape_file_archive_file_id_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX tape_file_archive_file_id_idx ON public.tape_file USING btree (archive_file_id);


--
-- Name: tape_file_vid_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX tape_file_vid_idx ON public.tape_file USING btree (vid);


--
-- Name: tape_lli_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX tape_lli_idx ON public.tape USING btree (logical_library_id);


--
-- Name: tape_mti_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX tape_mti_idx ON public.tape USING btree (media_type_id);


--
-- Name: tape_pool_tpn_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX tape_pool_tpn_ci_un_idx ON public.tape_pool USING btree (lower((tape_pool_name)::text));


--
-- Name: tape_pool_tpn_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX tape_pool_tpn_un_idx ON public.tape_pool USING btree (tape_pool_name);


--
-- Name: tape_pool_voi_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX tape_pool_voi_idx ON public.tape_pool USING btree (virtual_organization_id);


--
-- Name: tape_state_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX tape_state_idx ON public.tape USING btree (tape_state);


--
-- Name: tape_tape_pool_id_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX tape_tape_pool_id_idx ON public.tape USING btree (tape_pool_id);


--
-- Name: tape_vid_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX tape_vid_ci_un_idx ON public.tape USING btree (lower((vid)::text));


--
-- Name: virtual_org_din_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE INDEX virtual_org_din_idx ON public.virtual_organization USING btree (disk_instance_name);


--
-- Name: virtual_org_von_ci_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX virtual_org_von_ci_un_idx ON public.virtual_organization USING btree (lower((virtual_organization_name)::text));


--
-- Name: virtual_org_von_un_idx; Type: INDEX; Schema: public; Owner: cta
--

CREATE UNIQUE INDEX virtual_org_von_un_idx ON public.virtual_organization USING btree (virtual_organization_name);


--
-- Name: archive_file archive_file_din_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.archive_file
    ADD CONSTRAINT archive_file_din_fk FOREIGN KEY (disk_instance_name) REFERENCES public.disk_instance(disk_instance_name);


--
-- Name: archive_file archive_file_storage_class_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.archive_file
    ADD CONSTRAINT archive_file_storage_class_fk FOREIGN KEY (storage_class_id) REFERENCES public.storage_class(storage_class_id);


--
-- Name: archive_route archive_route_storage_class_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.archive_route
    ADD CONSTRAINT archive_route_storage_class_fk FOREIGN KEY (storage_class_id) REFERENCES public.storage_class(storage_class_id);


--
-- Name: archive_route archive_route_tape_pool_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.archive_route
    ADD CONSTRAINT archive_route_tape_pool_fk FOREIGN KEY (tape_pool_id) REFERENCES public.tape_pool(tape_pool_id);


--
-- Name: disk_instance_space disk_instance_space_din_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.disk_instance_space
    ADD CONSTRAINT disk_instance_space_din_fk FOREIGN KEY (disk_instance_name) REFERENCES public.disk_instance(disk_instance_name);


--
-- Name: disk_system disk_system_din_disn_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.disk_system
    ADD CONSTRAINT disk_system_din_disn_fk FOREIGN KEY (disk_instance_name, disk_instance_space_name) REFERENCES public.disk_instance_space(disk_instance_name, disk_instance_space_name);


--
-- Name: file_recycle_log file_recycle_log_sc_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.file_recycle_log
    ADD CONSTRAINT file_recycle_log_sc_fk FOREIGN KEY (storage_class_id) REFERENCES public.storage_class(storage_class_id);


--
-- Name: file_recycle_log file_recycle_log_vid_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.file_recycle_log
    ADD CONSTRAINT file_recycle_log_vid_fk FOREIGN KEY (vid) REFERENCES public.tape(vid);


--
-- Name: requester_activity_mount_rule rqster_act_rule_din_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_activity_mount_rule
    ADD CONSTRAINT rqster_act_rule_din_fk FOREIGN KEY (disk_instance_name) REFERENCES public.disk_instance(disk_instance_name);


--
-- Name: requester_activity_mount_rule rqster_act_rule_mnt_plc_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_activity_mount_rule
    ADD CONSTRAINT rqster_act_rule_mnt_plc_fk FOREIGN KEY (mount_policy_name) REFERENCES public.mount_policy(mount_policy_name);


--
-- Name: requester_group_mount_rule rqster_grp_rule_din_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_group_mount_rule
    ADD CONSTRAINT rqster_grp_rule_din_fk FOREIGN KEY (disk_instance_name) REFERENCES public.disk_instance(disk_instance_name);


--
-- Name: requester_group_mount_rule rqster_grp_rule_mnt_plc_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_group_mount_rule
    ADD CONSTRAINT rqster_grp_rule_mnt_plc_fk FOREIGN KEY (mount_policy_name) REFERENCES public.mount_policy(mount_policy_name);


--
-- Name: requester_mount_rule rqster_rule_din_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_mount_rule
    ADD CONSTRAINT rqster_rule_din_fk FOREIGN KEY (disk_instance_name) REFERENCES public.disk_instance(disk_instance_name);


--
-- Name: requester_mount_rule rqster_rule_mnt_plc_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.requester_mount_rule
    ADD CONSTRAINT rqster_rule_mnt_plc_fk FOREIGN KEY (mount_policy_name) REFERENCES public.mount_policy(mount_policy_name);


--
-- Name: storage_class storage_class_voi_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.storage_class
    ADD CONSTRAINT storage_class_voi_fk FOREIGN KEY (virtual_organization_id) REFERENCES public.virtual_organization(virtual_organization_id);


--
-- Name: tape_file tape_file_archive_file_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape_file
    ADD CONSTRAINT tape_file_archive_file_fk FOREIGN KEY (archive_file_id) REFERENCES public.archive_file(archive_file_id);


--
-- Name: tape_file tape_file_tape_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape_file
    ADD CONSTRAINT tape_file_tape_fk FOREIGN KEY (vid) REFERENCES public.tape(vid);


--
-- Name: tape tape_logical_library_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape
    ADD CONSTRAINT tape_logical_library_fk FOREIGN KEY (logical_library_id) REFERENCES public.logical_library(logical_library_id);


--
-- Name: tape tape_media_type_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape
    ADD CONSTRAINT tape_media_type_fk FOREIGN KEY (media_type_id) REFERENCES public.media_type(media_type_id);


--
-- Name: tape_pool tape_pool_vo_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape_pool
    ADD CONSTRAINT tape_pool_vo_fk FOREIGN KEY (virtual_organization_id) REFERENCES public.virtual_organization(virtual_organization_id);


--
-- Name: tape tape_tape_pool_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.tape
    ADD CONSTRAINT tape_tape_pool_fk FOREIGN KEY (tape_pool_id) REFERENCES public.tape_pool(tape_pool_id);


--
-- Name: virtual_organization virtual_organization_din_fk; Type: FK CONSTRAINT; Schema: public; Owner: cta
--

ALTER TABLE ONLY public.virtual_organization
    ADD CONSTRAINT virtual_organization_din_fk FOREIGN KEY (disk_instance_name) REFERENCES public.disk_instance(disk_instance_name);


--
-- PostgreSQL database dump complete
--

