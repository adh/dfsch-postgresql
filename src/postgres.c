#include "dfsch-ext/postgres.h"
#include <postgresql/libpq-fe.h>
#include <dfsch/load.h>
#include <dfsch/hash.h>
#include <dfsch/number.h>


typedef struct pg_conn_t {
  dfsch_type_t* type;
  PGconn* conn;
  int open;
} pg_conn_t;

dfsch_type_t pg_conn_type = {
  DFSCH_STANDARD_TYPE,
  NULL,
  sizeof(pg_conn_t),
  "pg:connection",
  NULL,
  NULL,
  NULL,
  NULL
};


static void conn_finalizer(pg_conn_t* conn, void* cd){
  if (conn->open){
    PQfinish(conn->conn);
  }
}

pg_conn_t* pg_conn(dfsch_object_t* conn){
  if (DFSCH_TYPE_OF(conn) != &pg_conn_type){
    dfsch_error("postgres:not-a-connection", conn);
  }

  if (!((pg_conn_t*)conn)->open){
    dfsch_error("postgres:connection-already-closed", conn);
  }

  return (pg_conn_t*)conn;
}

#define PG_CONN_ARG(al, name)                           \
  DFSCH_GENERIC_ARG(al, name, pg_conn_t*, pg_conn)

typedef struct pg_result_t {
  dfsch_type_t* type;
  PGresult* res;
  int open;
  int row;
} pg_result_t;

dfsch_type_t pg_result_type = {
  DFSCH_STANDARD_TYPE,
  NULL,
  sizeof(pg_result_t),
  "pg:result",
  NULL,
  NULL,
  NULL,
  NULL
};


static void result_finalizer(pg_result_t* res, void* cd){
  if (res->open){
    PQclear(res->res);
  }
}

pg_result_t* pg_result(dfsch_object_t* res){
  if (DFSCH_TYPE_OF(res) != &pg_result_type){
    dfsch_error("postgres:not-a-connection", res);
  }

  if (!((pg_conn_t*)res)->open){
    dfsch_error("postgres:connection-already-closed", res);
  }

  return (pg_result_t*)res;
}

#define PG_RESULT_ARG(al, name)                         \
  DFSCH_GENERIC_ARG(al, name, pg_result_t*, pg_result)

static dfsch_object_t* pg_make_result(PGresult* res, char*statement){
  pg_result_t* r;
  dfsch_object_t* err;

  if (!res){
    dfsch_error("postgres:could-not-create-result-object", 
                dfsch_make_string_cstr(statement));
  }

  switch (PQresultStatus(res)){
  case PGRES_EMPTY_QUERY:
  case PGRES_COMMAND_OK:
    PQclear(res);
    return NULL;

  case PGRES_COPY_OUT:
  case PGRES_COPY_IN:
    PQclear(res);
    dfsch_error("postgres:copy-not-supported", 
                dfsch_make_string_cstr(statement));
    break;

  case PGRES_TUPLES_OK:
    break;

  default:
    err = dfsch_make_string_cstr(PQresultErrorMessage(res));
    PQclear(res);
    dfsch_error("postgres:error", 
                dfsch_list(2,
                           err,
                           dfsch_make_string_cstr(statement)));
  }

  r = (pg_result_t*)dfsch_make_object(&pg_result_type);

  r->open = 1;
  r->row = -1; // XXX
  r->res = res;

  return (dfsch_object_t*)r;
}




static dfsch_object_t* pg_connect(void* baton,
                                  dfsch_object_t* args,
                                  dfsch_tail_escape_t* esc){
  char* conninfo;
  pg_conn_t* conn;
  dfsch_object_t* err_msg;

  DFSCH_STRING_ARG_OPT(args, conninfo, "");
  DFSCH_ARG_END(args);

  conn = (pg_conn_t*) dfsch_make_object(&pg_conn_type);

  conn->conn = PQconnectdb(conninfo);

  if (PQstatus(conn->conn) == CONNECTION_BAD){
    err_msg = dfsch_make_string_cstr(PQerrorMessage(conn->conn));
    PQfinish(conn->conn);
    dfsch_error("postgres:cannot-connect", err_msg);
  }

  conn->open = 1;
  GC_REGISTER_FINALIZER(conn, (GC_finalization_proc)conn_finalizer, NULL, 
                        NULL, NULL);

  return (dfsch_object_t*)conn;
}

static dfsch_object_t* pg_finish(void* baton,
                                 dfsch_object_t* args,
                                 dfsch_tail_escape_t* esc){
  pg_conn_t* conn;

  PG_CONN_ARG(args, conn);
  DFSCH_ARG_END(args);

  conn->open = 0;
  PQfinish(conn->conn);

  return NULL;
}

static dfsch_object_t* pg_exec(void* baton,
                               dfsch_object_t* args,
                               dfsch_tail_escape_t* esc){
  pg_conn_t* conn;
  char* command;

  PG_CONN_ARG(args, conn);
  DFSCH_STRING_ARG(args, command);
  DFSCH_ARG_END(args);

  return pg_make_result(PQexec(conn->conn, command), command);
}


static dfsch_object_t* get_row_as_vector(pg_result_t* res){
  size_t i;
  size_t len;
  dfsch_object_t* vec;

  if (res->row < 0){
    return NULL;
  }

  len = PQnfields(res->res);

  vec = dfsch_make_vector(len, NULL);

  for (i = 0; i < len; i++){
    if (!PQgetisnull(res->res, res->row, i)){
      dfsch_vector_set(vec, i, dfsch_make_string_cstr(PQgetvalue(res->res, 
                                                                 res->row, 
                                                                 i)));
    }
  }

  return vec;
}

static dfsch_object_t* get_row_as_hash(pg_result_t* res){
  size_t i;
  size_t len;
  dfsch_object_t* hash;

  if (res->row < 0){
    return NULL;
  }

  len = PQnfields(res->res);

  hash = dfsch_hash_make(DFSCH_HASH_EQ);

  for (i = 0; i < len; i++){
    if (!PQgetisnull(res->res, res->row, i)){
      dfsch_hash_set(hash,
                     dfsch_make_symbol(PQfname(res->res, i)),
                     dfsch_make_string_cstr(PQgetvalue(res->res, 
                                                       res->row, 
                                                       i)));
    } else {
      dfsch_hash_set(hash,
                     dfsch_make_symbol(PQfname(res->res, i)),
                     NULL);
    }
  }

  return hash;
}


static dfsch_object_t* pg_step(void* baton,
                               dfsch_object_t* args,
                               dfsch_tail_escape_t* esc){
  dfsch_object_t* format;
  pg_result_t* res;
  
  PG_RESULT_ARG(args, res);
  DFSCH_OBJECT_ARG_OPT(args, format, NULL);
  DFSCH_ARG_END(args);

  res->row++;

  if (res->row < PQntuples(res->res)){ 
    if (format == NULL){
      return dfsch_sym_true();
    } else if (dfsch_compare_symbol(format, "vector")) {
      return get_row_as_vector(res);
    } else if (dfsch_compare_symbol(format, "hash")) {
      return get_row_as_hash(res);    
    } else {
      dfsch_error("postgres:unknown-format", format);
    }
  } else {
    res->open = 0;
    PQclear(res->res);
    return NULL;
  }
}

static dfsch_object_t* pg_get_row(void* baton,
                                  dfsch_object_t* args,
                                  dfsch_tail_escape_t* esc){
  dfsch_object_t* format;
  pg_result_t* res;
  
  PG_RESULT_ARG(args, res);
  DFSCH_OBJECT_ARG_OPT(args, format, NULL);
  DFSCH_ARG_END(args);

  if (format == NULL || dfsch_compare_symbol(format, "vector")) {
    return get_row_as_vector(res);
  } else if (dfsch_compare_symbol(format, "hash")) {
    return get_row_as_hash(res);    
  } else {
    dfsch_error("postgres:unknown-format", format);
  }
}

static dfsch_object_t* pg_get_names(void* baton,
                                    dfsch_object_t* args,
                                    dfsch_tail_escape_t* esc){
  pg_result_t* res;
  size_t i;
  size_t len;
  dfsch_object_t* vec;
  
  PG_RESULT_ARG(args, res);
  DFSCH_ARG_END(args);

  if (res->row < 0){
    return NULL;
  }

  len = PQnfields(res->res);

  vec = dfsch_make_vector(len, NULL);

  for (i = 0; i < len; i++){
    dfsch_vector_set(vec, i, dfsch_make_string_cstr(PQfname(res->res, i)));
  }

  return vec;
}

static dfsch_object_t* pg_get_value(void* baton,
                                    dfsch_object_t* args,
                                    dfsch_tail_escape_t* esc){
  pg_result_t* res;
  int column;
  int row;

  PG_RESULT_ARG(args, res);
  DFSCH_LONG_ARG(args, column);
  DFSCH_LONG_ARG_OPT(args, row, res->row);
  DFSCH_ARG_END(args);
  
  if (row < 0 || row > PQntuples(res->res)){
    dfsch_error("postgres:row-number-out-of-range",
                dfsch_list(2,
                           dfsch_make_number_from_long(row),
                           dfsch_make_number_from_long(PQntuples(res->res))));
  }
  if (column < 0 || column > PQnfields(res->res)){
    dfsch_error("postgres:column-number-out-of-range",
                dfsch_list(2,
                           dfsch_make_number_from_long(row),
                           dfsch_make_number_from_long(PQnfields(res->res))));
  }
  
  if (PQgetisnull(res->res, row, column)){
    return NULL;
  }

  return dfsch_make_string_cstr(PQgetvalue(res->res, row, column));

}

static dfsch_object_t* pg_close_result(void* baton,
                                       dfsch_object_t* args,
                                       dfsch_tail_escape_t* esc){
  pg_result_t* res;
  
  PG_RESULT_ARG(args, res);
  DFSCH_ARG_END(args);

  res->open = 0;
  PQclear(res->res);
  return NULL;
}



void dfsch_module_postgres_register(dfsch_object_t* env){
  dfsch_provide(env, "postgres");


  dfsch_define_cstr(env, "pg:<connection>", &pg_conn_type);
  dfsch_define_cstr(env, "pg:<result>", &pg_result_type);

  dfsch_define_cstr(env, "pg:connect", 
                    dfsch_make_primitive(pg_connect, NULL));
  dfsch_define_cstr(env, "pg:finish", 
                    dfsch_make_primitive(pg_finish, NULL));
  dfsch_define_cstr(env, "pg:exec", 
                    dfsch_make_primitive(pg_exec, NULL));
  dfsch_define_cstr(env, "pg:step", 
                    dfsch_make_primitive(pg_step, NULL));
  dfsch_define_cstr(env, "pg:close-result", 
                    dfsch_make_primitive(pg_close_result, NULL));
  dfsch_define_cstr(env, "pg:get-row", 
                    dfsch_make_primitive(pg_get_row, NULL));
  dfsch_define_cstr(env, "pg:get-names", 
                    dfsch_make_primitive(pg_get_names, NULL));
}
