/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "isal_load.h"

#ifndef ISAL_LIBRARY
#define ISAL_LIBRARY "libisal.so"
#endif

IsaLibLoader *isaLoader;

/**
 *  isal_load.c
 *  Utility of loading the ISA-L library and the required functions.
 *  Building of this codes won't rely on any ISA-L source codes, but running
 *  into this will rely on successfully loading of the dynamic library.
 *
 */

static const char *load_functions() {
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_mul), "gf_mul");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_inv), "gf_inv");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_gen_rs_matrix), "gf_gen_rs_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_gen_cauchy_matrix),
                         "gf_gen_cauchy1_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_invert_matrix), "gf_invert_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_vect_mul), "gf_vect_mul");

  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_init_tables), "ec_init_tables");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_encode_data), "ec_encode_data");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_encode_data_update),
                         "ec_encode_data_update");
  return NULL;
}

void load_erasurecode_lib(char *err, size_t err_len) {
  const char *errMsg;
  const char *library = NULL;
  Dl_info dl_info;

  err[0] = '\0';

  if (isaLoader != NULL) {
    return;
  }
  isaLoader = calloc(1, sizeof(IsaLibLoader));
  memset(isaLoader, 0, sizeof(IsaLibLoader));

  // Load Intel ISA-L
  isaLoader->libec = dlopen(ISAL_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (isaLoader->libec == NULL) {
    snprintf(err, err_len, "Failed to load %s (%s)",
             ISAL_LIBRARY, dlerror());
    return;
  }
  // Clear any existing error
  dlerror();

  errMsg = load_functions(isaLoader->libec);
  if (errMsg != NULL) {
    snprintf(err, err_len, "Loading functions from ISA-L failed: %s",
             errMsg);
  }

  if (dladdr(isaLoader->ec_encode_data, &dl_info)) {
    library = dl_info.dli_fname;
  }

  if (library == NULL) {
    library = ISAL_LIBRARY;
  }

  isaLoader->libname = strdup(library);
}
