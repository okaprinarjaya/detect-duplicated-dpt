/* lcstest.c
 *
 * Copyright (C) 2015 - 2019, Helmut Wollmersdorfer, all rights reserved.
 *
*/


#include <stdio.h>
#include <limits.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <nmmintrin.h>

int count_bits_fast(uint64_t bits);
int llcs_asci_t_f (unsigned char * a, unsigned char * b, uint32_t alen, uint32_t blen);

static const uint64_t width = 64;

int count_bits_fast(uint64_t bits) {
  // 12 operations
  bits = bits - ((bits >> 1) & 0x5555555555555555ull);
  bits = (bits & 0x3333333333333333ull) + ((bits >> 2) & 0x3333333333333333ull);
  // (bytesof(bits) -1) * bitsofbyte = (8-1)*8 = 56 -------------------------------vv
  return ((bits + (bits >> 4) & 0x0f0f0f0f0f0f0f0full) * 0x0101010101010101ull) >> 56;
}

// use table (16 LoCs, 11 netLoCs)
// O(12*ceil(m/w) + 5*m + 7*ceil(m/w)*n)= 12*1 + 5*10 + 7*1*11 = 12 + 50 + 77 = 139
int llcs_asci_t_f (unsigned char * a, unsigned char * b, uint32_t alen, uint32_t blen) {

  static uint64_t posbits[128] = { 0 };
  uint64_t i;

  for (i=0; i < 128; i++){
    posbits[i] = 0;
  }


  // 5 instr * ceil(m/w) * m
  for (i=0; i < alen; i++){
    posbits[(unsigned int)a[i]] |= 0x1ull << (i % width);
  }

  uint64_t v = ~0ull;
  // 7 instr * ceil(m/w)*n
  for (i=0; i < blen; i++){
    uint64_t p = posbits[(unsigned int)b[i]];
    uint64_t u = v & p;
    v = (v + u) | (v - u);
  }
  // 12 instr * ceil(m/w)
  return count_bits_fast(~v);
}
