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

#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifndef ISAL_INCLUDE_ISAL_LOAD_H_
#define ISAL_INCLUDE_ISAL_LOAD_H_


// For gf_util.h
typedef unsigned char (*__d_gf_mul)(unsigned char, unsigned char);

typedef unsigned char (*__d_gf_inv)(unsigned char);

typedef void (*__d_gf_gen_rs_matrix)(unsigned char *, int, int);

typedef void (*__d_gf_gen_cauchy_matrix)(unsigned char *, int, int);

typedef int (*__d_gf_invert_matrix)(unsigned char *, unsigned char *,
                                    const int);

typedef int (*__d_gf_vect_mul)(int, unsigned char *, void *, void *);

// For erasure_code.h
typedef void (*__d_ec_init_tables)(int, int, unsigned char *, unsigned char *);

typedef void (*__d_ec_encode_data)(int, int, int, unsigned char *,
                                   unsigned char **, unsigned char **);

typedef void (*__d_ec_encode_data_update)(int, int, int, int, unsigned char *,
                                          unsigned char *, unsigned char **);

typedef struct __IsaLibLoader {
  // The loaded library handle
  void *libec;
  char *libname;

  __d_gf_mul gf_mul;
  __d_gf_inv gf_inv;
  __d_gf_gen_rs_matrix gf_gen_rs_matrix;
  __d_gf_gen_cauchy_matrix gf_gen_cauchy_matrix;
  __d_gf_invert_matrix gf_invert_matrix;
  __d_gf_vect_mul gf_vect_mul;
  __d_ec_init_tables ec_init_tables;
  __d_ec_encode_data ec_encode_data;
  __d_ec_encode_data_update ec_encode_data_update;
} IsaLibLoader;

extern IsaLibLoader *isaLoader;

/**
 * A helper function to dlsym a 'symbol' from a given library-handle.
 */

static __attribute__((unused))
void *myDlsym(void *handle, const char *symbol) {
  void *func_ptr = dlsym(handle, symbol);
  return func_ptr;
}

/* A helper macro to dlsym the requisite dynamic symbol in NON-JNI env. */
#define EC_LOAD_DYNAMIC_SYMBOL(func_ptr, symbol) \
  if ((func_ptr = myDlsym(isaLoader->libec, symbol)) == NULL) { \
    return "Failed to load symbol" symbol; \
  }

/**
 * Initialize and load erasure code library, returning error message if any.
 *
 * @param err     The err message buffer.
 * @param err_len The length of the message buffer.
 */
void load_erasurecode_lib(char *err, size_t err_len);

#endif  // ISAL_INCLUDE_ISAL_LOAD_H_
