/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/ValueFactory.hpp"
#include "common/serializeio.h"

#include <cstdlib>

using namespace voltdb;

class TableTupleELTTest : public Test {

  protected:
    // some utility functions to verify
    size_t maxElSize(std::vector<uint16_t> &keep_offsets);
    size_t serElSize(std::vector<uint16_t> &keep_offsets, uint8_t*, char*, bool nulls=false);
    void verSer(int, char*);

  public:
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull;
    TupleSchema *m_schema;

    void addToSchema(ValueType vt, bool allownull) {
        columnTypes.push_back(vt);
        columnLengths.push_back(NValue::getTupleStorageSize(vt));
        columnAllowNull.push_back(allownull);
    }

    TableTupleELTTest() {
        bool allownull = true;

        // note that maxELSize() cares about the string tuple offsets..

        // set up a schema with each supported column type
        addToSchema(VALUE_TYPE_TINYINT, allownull);  // 0
        addToSchema(VALUE_TYPE_SMALLINT, allownull); // 1
        addToSchema(VALUE_TYPE_INTEGER, allownull);  // 2
        addToSchema(VALUE_TYPE_BIGINT, allownull);   // 3
        addToSchema(VALUE_TYPE_TIMESTAMP, allownull); // 4
        addToSchema(VALUE_TYPE_DECIMAL, allownull);   // 5

        // need explicit lengths for varchar columns
        columnTypes.push_back(VALUE_TYPE_VARCHAR);  // 6
        columnLengths.push_back(15);
        columnAllowNull.push_back(allownull);

        columnTypes.push_back(VALUE_TYPE_VARCHAR);   // 7
        columnLengths.push_back(UNINLINEABLE_OBJECT_LENGTH * 2);
        columnAllowNull.push_back(allownull);

        m_schema = TupleSchema::createTupleSchema(
            columnTypes, columnLengths, columnAllowNull, true /* allow inlined strs */);
    }

    ~TableTupleELTTest() {
        TupleSchema::freeTupleSchema(m_schema);
    }

};


// helper to make a schema, a tuple and calculate EL size
size_t
TableTupleELTTest::maxElSize(std::vector<uint16_t> &keep_offsets)
{
    TableTuple *tt;
    TupleSchema *ts;
    char buf[1024]; // tuple data

    ts = TupleSchema::createTupleSchema(m_schema, keep_offsets);
    tt = new TableTuple(buf, ts);

    // if the tuple includes strings, add some content
    // assuming all ELT tuples were allocated for persistent
    // storage and choosing set* api accordingly here.
    if (ts->columnCount() > 6) {
        NValue nv = ValueFactory::getStringValue("ABCDEabcde"); // 10 char
        tt->setNValueAllocateForObjectCopies(6, nv, NULL);
        nv.free();
    }
    if (ts->columnCount() > 7) {
        NValue nv = ValueFactory::getStringValue("abcdeabcdeabcdeabcde"); // 20 char
        tt->setNValueAllocateForObjectCopies(7, nv, NULL);
        nv.free();
    }

    // The function under test!
    size_t sz = tt->maxELTSerializationSize();

    // and cleanup
    tt->freeObjectColumns();
    delete tt;
    TupleSchema::freeTupleSchema(ts);

    return sz;
}


/*
 * Verify that the max tuple size returns expected result
 */
TEST_F(TableTupleELTTest, maxELTSerSize_tiny) {

    // create a schema by selecting a column from the super-set.
    size_t sz = 0;
    std::vector<uint16_t> keep_offsets;
    uint16_t i = 0;

    // just tinyint in schema
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(8, sz);

    // tinyint + smallint
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(16, sz);

    // + integer
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(24, sz);

    // + bigint
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(32, sz);

    // + timestamp
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(40, sz);

    // + decimal
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(40 + 4 + 1 + 1 + 38, sz);  // length, radix pt, sign, prec.

    // + first varchar
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(84 + 14, sz); // length, 10 chars

    // + second varchar
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(98 + 24, sz); // length, 20 chars
}


// helper to make a schema, a tuple and serialize to a buffer
size_t
TableTupleELTTest::serElSize(std::vector<uint16_t> &keep_offsets,
                             uint8_t *nullArray, char *dataPtr, bool nulls)
{
    TableTuple *tt;
    TupleSchema *ts;
    char buf[1024]; // tuple data

    ts = TupleSchema::createTupleSchema(m_schema, keep_offsets);
    tt = new TableTuple(buf, ts);

    // assuming all ELT tuples were allocated for persistent
    // storage and choosing set* api accordingly here.

    switch (ts->columnCount()) {
        // note my sophisticated and clever use of fall through
      case 8:
      {
          NValue nv = ValueFactory::getStringValue("abcdeabcdeabcdeabcde"); // 20 char
          if (nulls) { nv.free(); nv.setNull(); }
          tt->setNValueAllocateForObjectCopies(7, nv, NULL);
          nv.free();
      }
      case 7:
      {
          NValue nv = ValueFactory::getStringValue("ABCDEabcde"); // 10 char
          if (nulls) { nv.free(); nv.setNull(); }
          tt->setNValueAllocateForObjectCopies(6, nv, NULL);
          nv.free();
      }
      case 6:
      {
          NValue nv = ValueFactory::getDecimalValueFromString("-12.34");
          if (nulls) { nv.free(); nv.setNull(); }
          tt->setNValueAllocateForObjectCopies(5, nv, NULL);
          nv.free();
      }
      case 5:
      {
          NValue nv = ValueFactory::getTimestampValue(9999);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(4, nv, NULL);
          nv.free();
      }
      case 4:
      {
          NValue nv = ValueFactory::getBigIntValue(1024);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(3, nv, NULL);
          nv.free();
      }
      case 3:
      {
          NValue nv = ValueFactory::getIntegerValue(512);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(2, nv, NULL);
          nv.free();
      }
      case 2:
      {
          NValue nv = ValueFactory::getSmallIntValue(256);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(1, nv, NULL);
          nv.free();
      }
      case 1:
      {
          NValue nv = ValueFactory::getTinyIntValue(120);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(0, nv, NULL);
          nv.free();
      }
      break;

      default:
        // this is an error in the test fixture.
        EXPECT_EQ(0,1);
        break;
    }

    // The function under test!
    size_t sz = tt->serializeToELT(0, nullArray, dataPtr);

    // and cleanup
    tt->freeObjectColumns();
    delete tt;
    TupleSchema::freeTupleSchema(ts);
    return sz;
}

// helper to verify the data that was serialized to the buffer
void
TableTupleELTTest::verSer(int cnt, char *data)
{
    ReferenceSerializeInput sin(data, 2048);

    if (cnt-- >= 0)
    {
        int64_t v = sin.readLong();
        EXPECT_EQ(120, v);
    }
    if (cnt-- >= 0)
    {
        int64_t v = sin.readLong();
        EXPECT_EQ(256, v);
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(512, sin.readLong());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(1024, sin.readLong());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(9999, sin.readLong());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(16, sin.readInt());
        EXPECT_EQ('-', sin.readChar());
        EXPECT_EQ('1', sin.readChar());
        EXPECT_EQ('2', sin.readChar());
        EXPECT_EQ('.', sin.readChar());
        EXPECT_EQ('3', sin.readChar());
        EXPECT_EQ('4', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
        EXPECT_EQ('0', sin.readChar());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(10, sin.readInt());
        EXPECT_EQ('A', sin.readChar());
        EXPECT_EQ('B', sin.readChar());
        EXPECT_EQ('C', sin.readChar());
        EXPECT_EQ('D', sin.readChar());
        EXPECT_EQ('E', sin.readChar());
        EXPECT_EQ('a', sin.readChar());
        EXPECT_EQ('b', sin.readChar());
        EXPECT_EQ('c', sin.readChar());
        EXPECT_EQ('d', sin.readChar());
        EXPECT_EQ('e', sin.readChar());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(20, sin.readInt());
        for (int ii =0; ii < 4; ++ii) {
            EXPECT_EQ('a', sin.readChar());
            EXPECT_EQ('b', sin.readChar());
            EXPECT_EQ('c', sin.readChar());
            EXPECT_EQ('d', sin.readChar());
            EXPECT_EQ('e', sin.readChar());
        }
    }
}

/*
 * Verify that tuple serialization produces expected content
 */
TEST_F(TableTupleELTTest, serToELT)
{
    uint8_t nulls[1] = {0};
    char data[2048];
    memset(data, 0, 2048);

    size_t sz = 0;
    std::vector<uint16_t> keep_offsets;
    uint16_t i = 0;

    // create a schema by selecting a column from the super-set.

    // tinyiny
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(8, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // tinyint + smallint
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(16, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + integer
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(24, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + bigint
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(32, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + timestamp
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(40, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + decimal
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(40 + 14 + 1 + 1 + 4, sz);  // length, radix pt, sign, prec.
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + first varchar
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(60 + 14, sz); // length, 10 chars
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + second varchar
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(74 + 24, sz); // length, 20 chars
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);
}


/* verify serialization of nulls */
TEST_F(TableTupleELTTest, serWithNulls)
{

    uint8_t nulls[1] = {0};
    char data[2048];
    memset(data, 0, 2048);

    size_t sz = 0;
    std::vector<uint16_t> keep_offsets;
    uint16_t i = 0;

    // tinyiny
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80, nulls[0]);

    // tinyint + smallint
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40, nulls[0]);  // all null

    // + integer
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20, nulls[0]);  // all null

    // + bigint
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10, nulls[0]);  // all null

    // + timestamp
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8, nulls[0]);  // all null

    // + decimal
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);  // length, radix pt, sign, prec.
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4, nulls[0]);  // all null

    // + first varchar
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz); // length, 10 chars
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4 | 0x2, nulls[0]);  // all null

    // + second varchar
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz); // length, 20 chars
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4 | 0x2 | 0x1, nulls[0]);  // all null
}


int main() {
    return TestSuite::globalInstance()->runAll();
}
