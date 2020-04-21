#include "erl_nif.h"
#include <string.h>

extern int llcs_asci_t_f(unsigned char * a, unsigned char * b, uint32_t alen, uint32_t blen);

static ERL_NIF_TERM lcs_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  int ret;
  unsigned char A[50];
  unsigned char B[50];

  enif_get_string(env, argv[0], A, 50, ERL_NIF_LATIN1);
  enif_get_string(env, argv[1], B, 50, ERL_NIF_LATIN1);

  uint32_t Alen = (uint32_t) strlen(A);
  uint32_t Blen = (uint32_t) strlen(B);
  ret = llcs_asci_t_f(A, B, Alen, Blen);

  return enif_make_int(env, ret);
}

static ErlNifFunc nif_funcs[] = {
  {"someone_lcs", 2, lcs_nif}
};

ERL_NIF_INIT(someone_lcs, nif_funcs, NULL, NULL, NULL, NULL)
