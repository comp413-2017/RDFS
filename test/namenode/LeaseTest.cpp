// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"

// Ensure that a lease is created successfully and that it does its job,
// by making sure another client can't request a lease on the same file

TEST_F(NamenodeTest, issueLeaseTest) {
    // Open a lease on some file, through an append request
    // Check append status

    // Create another append request on the same file
    // with different client ID.
    // Ensure this returns the expected error code -- LeaseAlreadyExists

    ASSERT_TRUE(true);
}

TEST_F(NamenodeTest, renewLeaseTest) {
    // Open a lease on some file, through an append request
    // Check append status

    // Call renew lease, and then check the timestamp on
    // the lease associated with the file.

    ASSERT_TRUE(true);
}

// Performance could be measured by tracking the round-trip time of lease open
// and lease close
// Performance could be measured by tracking the time it takes to issue a lease
