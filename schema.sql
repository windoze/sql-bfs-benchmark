create table entities
(
    entity_id   varchar(50)  not null
        primary key,
    entity_name varchar(100) not null,
    entity_type varchar(20)  not null
);

create index entities_entity_type_index
    on entities (entity_type);

create table entity_dep
(
    from_id   varchar(50) not null,
    to_id     varchar(50) not null,
    conn_type varchar(20) not null
);

create index entity_dep_conn_type_index
    on entity_dep (conn_type);

create index entity_dep_from_id_index
    on entity_dep (from_id);

create index entity_dep_to_id_index
    on entity_dep (to_id);

