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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "org_apache_hadoop.h"
#include "../include/isal_load.h"

#ifdef UNIX
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dlfcn.h>

#include "config.h"
#endif

#ifdef WINDOWS
#include <Windows.h>
#endif

IsaLibLoader* isaLoader;

/**
 *  isal_load.c
 *  Utility of loading the ISA-L library and the required functions.
 *  Building of this codes won't rely on any ISA-L source codes, but running
 *  into this will rely on successfully loading of the dynamic library.
 *
 */

static const char* load_functions() {
#ifdef UNIX
    EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_mul), "gf_mul");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_inv), "gf_inv");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_gen_rs_matrix), "gf_gen_rs_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_gen_cauchy_matrix), "gf_gen_cauchy1_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_invert_matrix), "gf_invert_matrix");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->gf_vect_mul), "gf_vect_mul");

  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_init_tables), "ec_init_tables");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_encode_data), "ec_encode_data");
  EC_LOAD_DYNAMIC_SYMBOL((isaLoader->ec_encode_data_update), "ec_encode_data_update");
#endif

#ifdef WINDOWS
    EC_LOAD_DYNAMIC_SYMBOL(__d_gf_mul, (isaLoader->gf_mul), "gf_mul");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_inv, (isaLoader->gf_inv), "gf_inv");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_gen_rs_matrix, (isaLoader->gf_gen_rs_matrix), "gf_gen_rs_matrix");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_gen_cauchy_matrix, (isaLoader->gf_gen_cauchy_matrix), "gf_gen_cauchy1_matrix");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_invert_matrix, (isaLoader->gf_invert_matrix), "gf_invert_matrix");
  EC_LOAD_DYNAMIC_SYMBOL(__d_gf_vect_mul, (isaLoader->gf_vect_mul), "gf_vect_mul");

  EC_LOAD_DYNAMIC_SYMBOL(__d_ec_init_tables, (isaLoader->ec_init_tables), "ec_init_tables");
  EC_LOAD_DYNAMIC_SYMBOL(__d_ec_encode_data, (isaLoader->ec_encode_data), "ec_encode_data");
  EC_LOAD_DYNAMIC_SYMBOL(__d_ec_encode_data_update, (isaLoader->ec_encode_data_update), "ec_encode_data_update");
#endif

    return NULL;
}

void load_erasurecode_lib(char* err, size_t err_len) {
    const char* errMsg;
    const char* library = NULL;
#ifdef UNIX
    Dl_info dl_info;
#else
    LPTSTR filename = NULL;
#endif

    err[0] = '\0';

    if (isaLoader != NULL) {
        return;
    }
    isaLoader = calloc(1, sizeof(IsaLibLoader));
    memset(isaLoader, 0, sizeof(IsaLibLoader));

    // Load Intel ISA-L
#ifdef UNIX
    isaLoader->libec = dlopen(HADOOP_ISAL_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (isaLoader->libec == NULL) {
    snprintf(err, err_len, "Failed to load %s (%s)",
                             HADOOP_ISAL_LIBRARY, dlerror());
    return;
  }
  // Clear any existing error
  dlerror();
#endif

#ifdef WINDOWS
    isaLoader->libec = LoadLibrary(HADOOP_ISAL_LIBRARY);
  if (isaLoader->libec == NULL) {
    snprintf(err, err_len, "Failed to load %s", HADOOP_ISAL_LIBRARY);
    return;
  }
#endif

    errMsg = load_functions(isaLoader->libec);
    if (errMsg != NULL) {
        snprintf(err, err_len, "Loading functions from ISA-L failed: %s", errMsg);
    }

#ifdef UNIX
    if(dladdr(isaLoader->ec_encode_data, &dl_info)) {
    library = dl_info.dli_fname;
  }
#else
    if (GetModuleFileName(isaLoader->libec, filename, 256) > 0) {
        library = filename;
    }
#endif

    if (library == NULL) {
        library = HADOOP_ISAL_LIBRARY;
    }

    isaLoader->libname = strdup(library);
}

int build_support_erasurecode() {
#ifdef HADOOP_ISAL_LIBRARY
    return 1;
#else
    return 0;
#endif
}
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

#ifndef _GF_UTIL_H
#define _GF_UTIL_H

/**
 *  gf_util.h
 *  Interface to functions for vector (block) multiplication in GF(2^8).
 *
 *  This file defines the interface to routines used in fast RAID rebuild and
 *  erasure codes.
 */


/**
 * Single element GF(2^8) multiply.
 *
 * @param a  Multiplicand a
 * @param b  Multiplicand b
 * @returns  Product of a and b in GF(2^8)
 */
unsigned char h_gf_mul(unsigned char a, unsigned char b);

/**
 * Single element GF(2^8) inverse.
 *
 * @param a  Input element
 * @returns  Field element b such that a x b = {1}
 */
unsigned char h_gf_inv(unsigned char a);

/**
 * Generate a matrix of coefficients to be used for encoding.
 *
 * Vandermonde matrix example of encoding coefficients where high portion of
 * matrix is identity matrix I and lower portion is constructed as 2^{i*(j-k+1)}
 * i:{0,k-1} j:{k,m-1}. Commonly used method for choosing coefficients in
 * erasure encoding but does not guarantee invertable for every sub matrix.  For
 * large k it is possible to find cases where the decode matrix chosen from
 * sources and parity not in erasure are not invertable. Users may want to
 * adjust for k > 5.
 *
 * @param a  [mxk] array to hold coefficients
 * @param m  number of rows in matrix corresponding to srcs + parity.
 * @param k  number of columns in matrix corresponding to srcs.
 * @returns  none
 */
void h_gf_gen_rs_matrix(unsigned char *a, int m, int k);

/**
 * Generate a Cauchy matrix of coefficients to be used for encoding.
 *
 * Cauchy matrix example of encoding coefficients where high portion of matrix
 * is identity matrix I and lower portion is constructed as 1/(i + j) | i != j,
 * i:{0,k-1} j:{k,m-1}.  Any sub-matrix of a Cauchy matrix should be invertable.
 *
 * @param a  [mxk] array to hold coefficients
 * @param m  number of rows in matrix corresponding to srcs + parity.
 * @param k  number of columns in matrix corresponding to srcs.
 * @returns  none
 */
void h_gf_gen_cauchy_matrix(unsigned char *a, int m, int k);

/**
 * Invert a matrix in GF(2^8)
 *
 * @param in  input matrix
 * @param out output matrix such that [in] x [out] = [I] - identity matrix
 * @param n   size of matrix [nxn]
 * @returns 0 successful, other fail on singular input matrix
 */
int h_gf_invert_matrix(unsigned char *in, unsigned char *out, const int n);

/**
 * GF(2^8) vector multiply by constant, runs appropriate version.
 *
 * Does a GF(2^8) vector multiply b = Ca where a and b are arrays and C
 * is a single field element in GF(2^8). Can be used for RAID6 rebuild
 * and partial write functions. Function requires pre-calculation of a
 * 32-element constant array based on constant C. gftbl(C) = {C{00},
 * C{01}, C{02}, ... , C{0f} }, {C{00}, C{10}, C{20}, ... , C{f0} }.
 * Len and src must be aligned to 32B.
 *
 * This function determines what instruction sets are enabled
 * and selects the appropriate version at runtime.
 *
 * @param len   Length of vector in bytes. Must be aligned to 32B.
 * @param gftbl Pointer to 32-byte array of pre-calculated constants based on C.
 * @param src   Pointer to src data array. Must be aligned to 32B.
 * @param dest  Pointer to destination data array. Must be aligned to 32B.
 * @returns 0 pass, other fail
 */
int h_gf_vect_mul(int len, unsigned char *gftbl, void *src, void *dest);


#endif //_GF_UTIL_H
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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/isal_load.h"
#include "../include/gf_util.h"

/**
 *  gf_util.c
 *  Implementation GF utilities based on ISA-L library.
 *
 */

unsigned char h_gf_mul(unsigned char a, unsigned char b) {
    return isaLoader->gf_mul(a, b);
}

unsigned char h_gf_inv(unsigned char a) {
    return isaLoader->gf_inv(a);
}

void h_gf_gen_rs_matrix(unsigned char *a, int m, int k) {
    isaLoader->gf_gen_rs_matrix(a, m, k);
}

void h_gf_gen_cauchy_matrix(unsigned char *a, int m, int k) {
    isaLoader->gf_gen_cauchy_matrix(a, m, k);
}

int h_gf_invert_matrix(unsigned char *in, unsigned char *out, const int n) {
    return isaLoader->gf_invert_matrix(in, out, n);
}

int h_gf_vect_mul(int len, unsigned char *gftbl, void *src, void *dest) {
    return isaLoader->gf_vect_mul(len, gftbl, src, dest);
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is a sample program illustrating how to use the Intel ISA-L library.
 * Note it's adapted from erasure_code_test.c test program, but trying to use
 * variable names and styles we're more familiar with already similar to Java
 * coders.
 */

#ifndef _ERASURE_CODER_H_
#define _ERASURE_CODER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MMAX 14
#define KMAX 10

typedef struct _IsalCoder {
    int verbose;
    int numParityUnits;
    int numDataUnits;
    int numAllUnits;
} IsalCoder;

typedef struct _IsalEncoder {
    IsalCoder coder;

    unsigned char gftbls[MMAX * KMAX * 32];

    unsigned char encodeMatrix[MMAX * KMAX];
} IsalEncoder;

typedef struct _IsalDecoder {
    IsalCoder coder;

    unsigned char encodeMatrix[MMAX * KMAX];

    // Below are per decode call
    unsigned char gftbls[MMAX * KMAX * 32];
    unsigned int decodeIndex[MMAX];
    unsigned char tmpMatrix[MMAX * KMAX];
    unsigned char invertMatrix[MMAX * KMAX];
    unsigned char decodeMatrix[MMAX * KMAX];
    unsigned char erasureFlags[MMAX];
    int erasedIndexes[MMAX];
    int numErased;
    int numErasedDataUnits;
    unsigned char* realInputs[MMAX];
} IsalDecoder;

void initCoder(IsalCoder* pCoder, int numDataUnits, int numParityUnits);

void allowVerbose(IsalCoder* pCoder, int flag);

void initEncoder(IsalEncoder* encoder, int numDataUnits, int numParityUnits);

void initDecoder(IsalDecoder* decoder, int numDataUnits, int numParityUnits);

void clearDecoder(IsalDecoder* decoder);

int encode(IsalEncoder* encoder, unsigned char** dataUnits,
           unsigned char** parityUnits, int chunkSize);

int decode(IsalDecoder* decoder, unsigned char** allUnits,
           int* erasedIndexes, int numErased,
           unsigned char** recoveredUnits, int chunkSize);

int generateDecodeMatrix(IsalDecoder* pCoder);

#endif //_ERASURE_CODER_H_
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "../include/erasure_code.h"
#include "../include/gf_util.h"
#include "../include/erasure_coder.h"
#include "../include/dump.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void initCoder(IsalCoder* pCoder, int numDataUnits, int numParityUnits) {
    pCoder->verbose = 0;
    pCoder->numParityUnits = numParityUnits;
    pCoder->numDataUnits = numDataUnits;
    pCoder->numAllUnits = numDataUnits + numParityUnits;
}

// 0 not to verbose, 1 to verbose
void allowVerbose(IsalCoder* pCoder, int flag) {
    pCoder->verbose = flag;
}

static void initEncodeMatrix(int numDataUnits, int numParityUnits,
                             unsigned char* encodeMatrix) {
    // Generate encode matrix, always invertible
    h_gf_gen_cauchy_matrix(encodeMatrix,
                           numDataUnits + numParityUnits, numDataUnits);
}

void initEncoder(IsalEncoder* pCoder, int numDataUnits,
                 int numParityUnits) {
    initCoder(&pCoder->coder, numDataUnits, numParityUnits);

    initEncodeMatrix(numDataUnits, numParityUnits, pCoder->encodeMatrix);

    // Generate gftbls from encode matrix
    h_ec_init_tables(numDataUnits, numParityUnits,
                     &pCoder->encodeMatrix[numDataUnits * numDataUnits],
                     pCoder->gftbls);

    if (pCoder->coder.verbose > 0) {
        dumpEncoder(pCoder);
    }
}

void initDecoder(IsalDecoder* pCoder, int numDataUnits,
                 int numParityUnits) {
    initCoder(&pCoder->coder, numDataUnits, numParityUnits);

    initEncodeMatrix(numDataUnits, numParityUnits, pCoder->encodeMatrix);
}

int encode(IsalEncoder* pCoder, unsigned char** dataUnits,
           unsigned char** parityUnits, int chunkSize) {
    int numDataUnits = pCoder->coder.numDataUnits;
    int numParityUnits = pCoder->coder.numParityUnits;
    int i;

    for (i = 0; i < numParityUnits; i++) {
        memset(parityUnits[i], 0, chunkSize);
    }

    h_ec_encode_data(chunkSize, numDataUnits, numParityUnits,
                     pCoder->gftbls, dataUnits, parityUnits);

    return 0;
}

// Return 1 when diff, 0 otherwise
static int compare(int* arr1, int len1, int* arr2, int len2) {
    int i;

    if (len1 == len2) {
        for (i = 0; i < len1; i++) {
            if (arr1[i] != arr2[i]) {
                return 1;
            }
        }
        return 0;
    }

    return 1;
}

static int processErasures(IsalDecoder* pCoder, unsigned char** inputs,
                           int* erasedIndexes, int numErased) {
    int i, r, ret, index;
    int numDataUnits = pCoder->coder.numDataUnits;
    int isChanged = 0;

    for (i = 0, r = 0; i < numDataUnits; i++, r++) {
        while (inputs[r] == NULL) {
            r++;
        }

        if (pCoder->decodeIndex[i] != r) {
            pCoder->decodeIndex[i] = r;
            isChanged = 1;
        }
    }

    for (i = 0; i < numDataUnits; i++) {
        pCoder->realInputs[i] = inputs[pCoder->decodeIndex[i]];
    }

    if (isChanged == 0 &&
        compare(pCoder->erasedIndexes, pCoder->numErased,
                erasedIndexes, numErased) == 0) {
        return 0; // Optimization, nothing to do
    }

    clearDecoder(pCoder);

    for (i = 0; i < numErased; i++) {
        index = erasedIndexes[i];
        pCoder->erasedIndexes[i] = index;
        pCoder->erasureFlags[index] = 1;
        if (index < numDataUnits) {
            pCoder->numErasedDataUnits++;
        }
    }

    pCoder->numErased = numErased;

    ret = generateDecodeMatrix(pCoder);
    if (ret != 0) {
        printf("Failed to generate decode matrix\n");
        return -1;
    }

    h_ec_init_tables(numDataUnits, pCoder->numErased,
                     pCoder->decodeMatrix, pCoder->gftbls);

    if (pCoder->coder.verbose > 0) {
        dumpDecoder(pCoder);
    }

    return 0;
}

int decode(IsalDecoder* pCoder, unsigned char** inputs,
           int* erasedIndexes, int numErased,
           unsigned char** outputs, int chunkSize) {
    int numDataUnits = pCoder->coder.numDataUnits;
    int i;

    processErasures(pCoder, inputs, erasedIndexes, numErased);

    for (i = 0; i < numErased; i++) {
        memset(outputs[i], 0, chunkSize);
    }

    h_ec_encode_data(chunkSize, numDataUnits, pCoder->numErased,
                     pCoder->gftbls, pCoder->realInputs, outputs);

    return 0;
}

// Clear variables used per decode call
void clearDecoder(IsalDecoder* decoder) {
    decoder->numErasedDataUnits = 0;
    decoder->numErased = 0;
    memset(decoder->gftbls, 0, sizeof(decoder->gftbls));
    memset(decoder->decodeMatrix, 0, sizeof(decoder->decodeMatrix));
    memset(decoder->tmpMatrix, 0, sizeof(decoder->tmpMatrix));
    memset(decoder->invertMatrix, 0, sizeof(decoder->invertMatrix));
    memset(decoder->erasureFlags, 0, sizeof(decoder->erasureFlags));
    memset(decoder->erasedIndexes, 0, sizeof(decoder->erasedIndexes));
}

// Generate decode matrix from encode matrix
int generateDecodeMatrix(IsalDecoder* pCoder) {
    int i, j, r, p;
    unsigned char s;
    int numDataUnits;

    numDataUnits = pCoder->coder.numDataUnits;

    // Construct matrix b by removing error rows
    for (i = 0; i < numDataUnits; i++) {
        r = pCoder->decodeIndex[i];
        for (j = 0; j < numDataUnits; j++) {
            pCoder->tmpMatrix[numDataUnits * i + j] =
                    pCoder->encodeMatrix[numDataUnits * r + j];
        }
    }

    h_gf_invert_matrix(pCoder->tmpMatrix,
                       pCoder->invertMatrix, numDataUnits);

    for (i = 0; i < pCoder->numErasedDataUnits; i++) {
        for (j = 0; j < numDataUnits; j++) {
            pCoder->decodeMatrix[numDataUnits * i + j] =
                    pCoder->invertMatrix[numDataUnits *
                                         pCoder->erasedIndexes[i] + j];
        }
    }

    for (p = pCoder->numErasedDataUnits; p < pCoder->numErased; p++) {
        for (i = 0; i < numDataUnits; i++) {
            s = 0;
            for (j = 0; j < numDataUnits; j++) {
                s ^= h_gf_mul(pCoder->invertMatrix[j * numDataUnits + i],
                              pCoder->encodeMatrix[numDataUnits *
                                                   pCoder->erasedIndexes[p] + j]);
            }

            pCoder->decodeMatrix[numDataUnits * p + i] = s;
        }
    }

    return 0;
}