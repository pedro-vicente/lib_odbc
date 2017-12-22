#ifndef clock_gettime_t_HH
#define clock_gettime_t_HH 1

#include <time.h>

/////////////////////////////////////////////////////////////////////////////////////////////////////
//clock_gettime_t
/////////////////////////////////////////////////////////////////////////////////////////////////////

class clock_gettime_t
{
public:
  clock_gettime_t();
  void start();
  void stop();
  void now(const char* s = 0);
private:
  timespec begin;
  timespec last;
  int running;
  int get_clock_gettime(struct timespec *spec);
};


#endif

