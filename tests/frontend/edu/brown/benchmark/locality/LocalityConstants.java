/***************************************************************************
 *   Copyright (C) 2010 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.benchmark.locality;

public abstract class LocalityConstants {
    
    // ----------------------------------------------------------------
    // TABLE INFORMATION
    // ----------------------------------------------------------------
    
    public static final String  TABLENAME_TABLEA = "TABLEA";
    public static final long    TABLESIZE_TABLEA = 1000000l; 
    public static final long    BATCHSIZE_TABLEA = 50l;
    
    public static final String  TABLENAME_TABLEB = "TABLEB";
    public static final long    TABLESIZE_TABLEB = Math.round(LocalityConstants.TABLESIZE_TABLEA * 100.0);
    public static final long    BATCHSIZE_TABLEB = 50l;

    public static final String[] TABLENAMES = {
        TABLENAME_TABLEA,
        TABLENAME_TABLEB,
    };
    
    // ----------------------------------------------------------------
    // STORED PROCEDURE INFORMATION
    // ----------------------------------------------------------------
    
    public static final int FREQUENCY_GET_LOCAL     = 80;
    public static final int FREQUENCY_SET_LOCAL     = 20;
    public static final int FREQUENCY_GET_REMOTE    = 0;
    public static final int FREQUENCY_SET_REMOTE    = 0;

}