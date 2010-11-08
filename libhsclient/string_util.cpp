
// vim:sw=2:ai

/*
 * Copyright (C) 2010 DeNA Co.,Ltd.. All rights reserved.
 * See COPYRIGHT.txt for details.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif /* HAVE_CONFIG_H */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include "string_util.hpp"

namespace dena {

string_wref
get_token(char *& wp, char *wp_end, char delim)
{
  char *const wp_begin = wp;
  char *const p = memchr_char(wp_begin, delim, wp_end - wp_begin);
  if (p == 0) {
    wp = wp_end;
    return string_wref(wp_begin, wp_end - wp_begin);
  }
  wp = p + 1;
  return string_wref(wp_begin, p - wp_begin);
}

template <typename T> T
atoi_tmpl_nocheck(const char *start, const char *finish)
{
  T v = 0;
  for (; start != finish; ++start) {
    const char c = *start;
    if (c < '0' || c > '9') {
      break;
    }
    v *= 10;
    v += static_cast<T>(c - '0');
  }
  return v;
}

uint32_t
atoi_uint32_nocheck(const char *start, const char *finish)
{
  return atoi_tmpl_nocheck<uint32_t>(start, finish);
}

void
append_uint32(string_buffer& buf, uint32_t v)
{
  char *const wp = buf.make_space(64);
  const int len = snprintf(wp, 64, "%lu", static_cast<unsigned long>(v));
  if (len > 0) {
    buf.space_wrote(len);
  }
}

std::string
to_stdstring(const char *format, ...)
{
  va_list ap;
  va_start(ap, format);

  std::string buf;
  buf.resize(64);
  const std::size_t required = vsnprintf(&buf[0], buf.capacity(), format, ap);
  if (required > buf.size())  {
    buf.resize(required);
    vsnprintf(&buf[0], buf.size(), format, ap);
  } else {
    buf.resize(required);
  }
  va_end(ap);
  return buf;
}

std::string
to_stdstring(uint32_t v)
{
  return to_stdstring("%lu", static_cast<unsigned long>(v));
}

int
errno_string(const char *s, int en, std::string& err_r)
{
#if defined(HAVE_POSIX_COMPLIANT_STRERROR_R)
  char buf[512];
  if (!strerror_r(en, buf, sizeof(buf))) {
    std::string tmp(to_stdstring("%s: %s", s, buf));
    err_r.swap(tmp);
    return en;
  }
#elif defined(HAVE_GNU_SPECIFIC_STRERROR_R)
  char buf[512];
  char *p = strerror_r(en, buf, sizeof(buf));
  if (p) {
    std::string tmp(to_stdstring("%s: %s", s, p));
    err_r.swap(tmp);
    return en;
  }
#endif
  std::string tmp(to_stdstring("%s: %d", s, en));
  err_r.swap(tmp);
  return en;
}

template <typename T> size_t
split_tmpl_arr(char delim, const T& buf, T *parts, size_t parts_len)
{
  typedef typename T::value_type value_type;
  size_t i = 0;
  value_type *start = buf.begin();
  value_type *const finish = buf.end();
  for (i = 0; i < parts_len; ++i) {
    value_type *const p = memchr_char(start, delim, finish - start);
    if (p == 0) {
      parts[i] = T(start, finish - start);
      ++i;
      break;
    }
    parts[i] = T(start, p - start);
    start = p + 1;
  }
  const size_t r = i;
  for (; i < parts_len; ++i) {
    parts[i] = T();
  }
  return r;
}

size_t
split(char delim, const string_ref& buf, string_ref *parts,
  size_t parts_len)
{
  return split_tmpl_arr(delim, buf, parts, parts_len);
}

size_t
split(char delim, const string_wref& buf, string_wref *parts,
  size_t parts_len)
{
  return split_tmpl_arr(delim, buf, parts, parts_len);
}

template <typename T, typename V> size_t
split_tmpl_vec(char delim, const T& buf, V& parts)
{
  typedef typename T::value_type value_type;
  size_t i = 0;
  value_type *start = buf.begin();
  value_type *const finish = buf.end();
  while (true) {
    value_type *const p = memchr_char(start, delim, finish - start);
    if (p == 0) {
      parts.push_back(T(start, finish - start));
      break;
    }
    parts.push_back(T(start, p - start));
    start = p + 1;
  }
  const size_t r = i;
  return r;
}

size_t
split(char delim, const string_ref& buf, std::vector<string_ref>& parts_r)
{
  return split_tmpl_vec(delim, buf, parts_r);
}

size_t
split(char delim, const string_wref& buf, std::vector<string_wref>& parts_r)
{
  return split_tmpl_vec(delim, buf, parts_r);
}

};

