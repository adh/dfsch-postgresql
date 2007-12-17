#include "dfsch-ext/postgres.h"
#include <dfsch/load.h>

void dfsch_module_postgres_register(dfsch_object_t* env){
  dfsch_provide(env, "postgres");
}
