// Copyright 2017 Rice University, COMP 413 2017

#define ELPP_FRESH_LOG_FILE
#define ELPP_THREAD_SAFE

#include <easylogging++.h>
#include <gtest/gtest.h>
#include <cstring>

extern "C" {
#include "dump.h"
#include "erasure_code.h"
#include "erasure_coder.h"
#include "gf_util.h"
#include "isal_load.h"
};

INITIALIZE_EASYLOGGINGPP

#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/test-log-conf.conf"

class ErasureCodeTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    chunkSize = 1024;
    numDataUnits = 6;
    numParityUnits = 3;
    int i, j;
    char err[256];
    size_t err_len = sizeof(err);

    load_erasurecode_lib(err, err_len);
    ASSERT_EQ(strlen(err), 0);

    dataUnits = reinterpret_cast<unsigned char **>(
        calloc(numDataUnits, sizeof(unsigned char *)));
    parityUnits = reinterpret_cast<unsigned char **>(
        calloc(numParityUnits, sizeof(unsigned char *)));
    backupUnits = reinterpret_cast<unsigned char **>(
        calloc(numParityUnits, sizeof(unsigned char *)));

    // Allocate and generate data units
    unsigned int seed = 135;
    for (i = 0; i < numDataUnits; i++) {
      dataUnits[i] = reinterpret_cast<unsigned char *>(
          calloc(chunkSize, sizeof(unsigned char)));
      for (j = 0; j < chunkSize; j++) {
        dataUnits[i][j] = rand_r(&seed);
      }
    }

    // Allocate and initialize parity units
    for (i = 0; i < numParityUnits; i++) {
      parityUnits[i] = reinterpret_cast<unsigned char *>(
          calloc(chunkSize, sizeof(unsigned char)));
      for (j = 0; j < chunkSize; j++) {
        parityUnits[i][j] = 0;
      }
    }

    // Initialize the encoder and decoder
    pEncoder = reinterpret_cast<IsalEncoder *>(
        malloc(sizeof(IsalEncoder)));
    memset(pEncoder, 0, sizeof(*pEncoder));
    initEncoder(pEncoder, numDataUnits, numParityUnits);

    pDecoder = reinterpret_cast<IsalDecoder *>(
        malloc(sizeof(IsalDecoder)));
    memset(pDecoder, 0, sizeof(*pDecoder));
    initDecoder(pDecoder, numDataUnits, numParityUnits);
  }

  int chunkSize;
  int numDataUnits;
  int numParityUnits;
  IsalEncoder *pEncoder;
  IsalDecoder *pDecoder;
  unsigned char **dataUnits;
  unsigned char **parityUnits;
  unsigned char **backupUnits;
};  // class ErasureCodeTest

TEST_F(ErasureCodeTest, CanRecover) {
  unsigned char *allUnits[MMAX];
  unsigned char *decodingOutput[2];
  int erasedIndexes[2], i;

  encode(pEncoder, dataUnits, parityUnits, chunkSize);

  memcpy(allUnits, dataUnits, numDataUnits * (sizeof(unsigned char*)));
  memcpy(allUnits + numDataUnits, parityUnits,
    numParityUnits * (sizeof(unsigned char*)));

  erasedIndexes[0] = 1;
  erasedIndexes[1] = 7;

  backupUnits[0] = allUnits[erasedIndexes[0]];
  backupUnits[1] = allUnits[erasedIndexes[1]];

  allUnits[0] = NULL;  // Not to read
  allUnits[erasedIndexes[0]] = NULL;
  allUnits[erasedIndexes[1]] = NULL;

  decodingOutput[0] = reinterpret_cast<unsigned char *>(malloc(chunkSize));
  decodingOutput[1] = reinterpret_cast<unsigned char *>(malloc(chunkSize));

  decode(pDecoder, allUnits, erasedIndexes, 2, decodingOutput, chunkSize);

  for (i = 0; i < pDecoder->numErased; i++)
    ASSERT_EQ(0, memcmp(decodingOutput[i], backupUnits[i], chunkSize));

  dumpDecoder(pDecoder);
}

int main(int argc, char **argv) {
  el::Configurations conf(LOG_CONFIG_FILE);
  el::Loggers::reconfigureAllLoggers(conf);
  el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  return res;
}
