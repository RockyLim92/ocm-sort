#ifndef _DEBUG_H_
#define _DEBUG_H_

//#define DBG_ENABLE 1

#ifdef DBG_ENABLE
#define DBG_P(fmt, args...) printf("[DBG_P][%s:%s:%d] " fmt, __FILE__ ,__func__, __LINE__,##args)
#else
#define DBG_P(fmt, args...)
#endif

#endif
