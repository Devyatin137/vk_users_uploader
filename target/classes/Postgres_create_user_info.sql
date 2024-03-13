-- Table: public.user_info

-- DROP TABLE IF EXISTS public.user_info;

CREATE TABLE IF NOT EXISTS public.user_info
(
    user_id bigint NOT NULL,
    user_f_name character varying(100) COLLATE pg_catalog."default",
    user_l_name character varying(100) COLLATE pg_catalog."default",
    user_b_date date,
    user_city character varying(50) COLLATE pg_catalog."default",
    user_contacts character varying(100) COLLATE pg_catalog."default",
    user_political smallint,
    user_religion character varying(100) COLLATE pg_catalog."default",
    user_has_mobile boolean,
    user_processed timestamp without time zone,
    CONSTRAINT user_info_pkey PRIMARY KEY (user_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.user_info
    OWNER to postgres;

COMMENT ON TABLE public.user_info
    IS 'Инфорация о пользователях ВК';