
#include "funcdata.h"
#include "go_asm.h"
#include "textflag.h"

#include "getg.h"

TEXT ·getg(SB), NOSPLIT, $0-8
    get_tls(CX)
    MOVQ    g(CX), AX
    MOVQ    AX, ret+0(FP)
    RET
