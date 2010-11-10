
// vim:sw=2:ai

/*
 * Copyright (C) 2010 DeNA Co.,Ltd.. All rights reserved.
 * See COPYRIGHT.txt for details.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "database.hpp"
#include "string_util.hpp"
#include "escape.hpp"
#include "mysql_incl.hpp"

#define DBG_KEY(x)
#define DBG_SHUT(x)
#define DBG_LOCK(x)
#define DBG_THR(x)
#define DBG_CMP(x)
#define DBG_FLD(x)
#define DBG_REFCNT(x)
#define DBG_DELETED

/* status variables */
unsigned long long int open_tables_count;
unsigned long long int close_tables_count;
unsigned long long int lock_tables_count;
unsigned long long int unlock_tables_count;
unsigned long long int index_exec_count;

namespace dena {

prep_stmt::prep_stmt()
  : dbctx(0), table_id(static_cast<size_t>(-1)),
    idxnum(static_cast<size_t>(-1))
{
}
prep_stmt::prep_stmt(dbcontext_i *c, size_t tbl, size_t idx,
  const retfields_type& rf)
  : dbctx(c), table_id(tbl), idxnum(idx), retfields(rf)
{
  if (dbctx) {
    dbctx->table_addref(table_id);
  }
}
prep_stmt::~prep_stmt()
{
  if (dbctx) {
    dbctx->table_release(table_id);
  }
}

prep_stmt::prep_stmt(const prep_stmt& x)
  : dbctx(x.dbctx), table_id(x.table_id), idxnum(x.idxnum),
  retfields(x.retfields)
{
  if (dbctx) {
    dbctx->table_addref(table_id);
  }
}

prep_stmt&
prep_stmt::operator =(const prep_stmt& x)
{
  if (this != &x) {
    if (dbctx) {
      dbctx->table_release(table_id);
    }
    dbctx = x.dbctx;
    table_id = x.table_id;
    idxnum = x.idxnum;
    retfields = x.retfields;
    if (dbctx) {
      dbctx->table_addref(table_id);
    }
  }
  return *this;
}

struct database : public database_i, private noncopyable {
  database(const config& c);
  virtual ~database();
  virtual dbcontext_ptr create_context(bool for_write) volatile;
  virtual void stop() volatile;
  virtual const config& get_conf() const volatile;
 public:
  int child_running;
 private:
  config conf;
};

struct tablevec_entry {
  TABLE *table;
  size_t refcount;
  tablevec_entry() : table(0), refcount(0) { }
};

struct expr_user_lock : private noncopyable {
  expr_user_lock(THD *thd, int timeout)
    : lck_key("handlersocket_wr", 16, &my_charset_latin1),
      lck_timeout(timeout),
      lck_func_get_lock(&lck_key, &lck_timeout),
      lck_func_release_lock(&lck_key)
  {
    lck_key.fix_fields(thd, 0);
    lck_timeout.fix_fields(thd, 0);
    lck_func_get_lock.fix_fields(thd, 0);
    lck_func_release_lock.fix_fields(thd, 0);
  }
  long long get_lock() {
    return lck_func_get_lock.val_int();
  }
  long long release_lock() {
    return lck_func_release_lock.val_int();
  }
 private:
  Item_string lck_key;
  Item_int lck_timeout;
  Item_func_get_lock lck_func_get_lock;
  Item_func_release_lock lck_func_release_lock;
};

struct dbcontext : public dbcontext_i, private noncopyable {
  dbcontext(volatile database *d, bool for_write);
  virtual ~dbcontext();
  virtual void init_thread(const void *stack_botton,
    volatile int& shutdown_flag);
  virtual void term_thread();
  virtual bool check_alive();
  virtual void lock_tables_if();
  virtual void unlock_tables_if();
  virtual bool get_commit_error();
  virtual void clear_error();
  virtual void close_tables_if();
  virtual void table_addref(size_t tbl_id);
  virtual void table_release(size_t tbl_id);
  virtual void cmd_open_index(dbcallback_i& cb, size_t pst_id, const char *dbn,
    const char *tbl, const char *idx, const char *retflds);
  virtual void cmd_exec_on_index(dbcallback_i& cb, const cmd_exec_args& args);
  virtual void cmd_authorization(dbcallback_i& cb, int type, const char *key );
  virtual void set_statistics(size_t num_conns, size_t num_active);
 private:
  int set_thread_message(const char *fmt, ...)
    __attribute__((format (printf, 2, 3)));
  void cmd_insert_internal(dbcallback_i& cb, const prep_stmt& pst,
    const string_ref *fvals, size_t fvalslen);
  void cmd_sql_internal(dbcallback_i& cb, const prep_stmt& pst,
    const string_ref *fvals, size_t fvalslen);
  void cmd_find_internal(dbcallback_i& cb, const prep_stmt& pst,
    ha_rkey_function find_flag, const cmd_exec_args& args);
  void resp_record(dbcallback_i& cb, TABLE *const table, const prep_stmt& pst);
  void dump_record(dbcallback_i& cb, TABLE *const table, const prep_stmt& pst);
  int modify_record(dbcallback_i& cb, TABLE *const table,
    const prep_stmt& pst, const cmd_exec_args& args, char mod_op,
    size_t& success_count);
 private:
  typedef std::vector<tablevec_entry> table_vec_type;
  typedef std::pair<std::string, std::string> table_name_type;
  typedef std::map<table_name_type, size_t> table_map_type;
 private:
  volatile database *const dbref;
  bool for_write_flag;
  THD *thd;
  MYSQL_LOCK *lock;
  bool lock_failed;
  std::auto_ptr<expr_user_lock> user_lock;
  int user_level_lock_timeout;
  bool user_level_lock_locked;
  bool commit_error;
  std::vector<char> info_message_buf;
  table_vec_type table_vec;
  table_map_type table_map;
  #if MYSQL_VERSION_ID >= 50505
  MDL_request *mdl_request;
  #else
  void *mdl_request;
  #endif
};

database::database(const config& c)
  : child_running(1), conf(c)
{
}

database::~database()
{
}

dbcontext_ptr
database::create_context(bool for_write) volatile
{
  return dbcontext_ptr(new dbcontext(this, for_write));
}

void
database::stop() volatile
{
  child_running = false;
}

const config&
database::get_conf() const volatile
{
  return const_cast<const config&>(conf);
}

database_ptr
database_i::create(const config& conf)
{
  return database_ptr(new database(conf));
}

dbcontext::dbcontext(volatile database *d, bool for_write)
  : dbref(d), for_write_flag(for_write), thd(0), lock(0), lock_failed(false),
    user_level_lock_timeout(0), user_level_lock_locked(false),
    commit_error(false), mdl_request(0)
{
  info_message_buf.resize(8192);
  user_level_lock_timeout = d->get_conf().get_int("wrlock_timeout", 12);
}

dbcontext::~dbcontext()
{
}

namespace {

int
wait_server_to_start(THD *thd, volatile int& shutdown_flag)
{
  int r = 0;
  DBG_SHUT(fprintf(stderr, "HNDSOCK wsts\n"));
  pthread_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started) {
    timespec abstime = { };
    set_timespec(abstime, 1);
    pthread_cond_timedwait(&COND_server_started, &LOCK_server_started,
      &abstime);
    pthread_mutex_unlock(&LOCK_server_started);
    pthread_mutex_lock(&thd->mysys_var->mutex);
    THD::killed_state st = thd->killed;
    pthread_mutex_unlock(&thd->mysys_var->mutex);
    DBG_SHUT(fprintf(stderr, "HNDSOCK wsts kst %d\n", (int)st));
    pthread_mutex_lock(&LOCK_server_started);
    if (st != THD::NOT_KILLED) {
      DBG_SHUT(fprintf(stderr, "HNDSOCK wsts kst %d break\n", (int)st));
      r = -1;
      break;
    }
    if (shutdown_flag) {
      DBG_SHUT(fprintf(stderr, "HNDSOCK wsts kst shut break\n"));
      r = -1;
      break;
    }
  }
  pthread_mutex_unlock(&LOCK_server_started);
  DBG_SHUT(fprintf(stderr, "HNDSOCK wsts done\n"));
  return r;
}

}; // namespace

void
dbcontext::init_thread(const void *stack_bottom, volatile int& shutdown_flag)
{
  DBG_THR(fprintf(stderr, "HNDSOCK init thread\n"));
  {
    my_thread_init();
    thd = new THD;
    thd->thread_stack = (char *)stack_bottom;
    DBG_THR(const size_t of = (char *)(&thd->thread_stack) - (char *)thd);
    DBG_THR(fprintf(stderr, "thread_stack = %p sz=%zu of=%zu\n",
      thd->thread_stack, sizeof(THD), of));
    thd->store_globals();
    thd->system_thread = static_cast<enum_thread_type>(1<<30UL);
    const NET v = { 0 };
    thd->net = v;
    if (for_write_flag) {
      #if MYSQL_VERSION_ID >= 50505
      thd->variables.option_bits |= OPTION_BIN_LOG;
      #else
      thd->options |= OPTION_BIN_LOG;
      #endif
      safeFree(thd->db);
      thd->db = 0;
      thd->db = my_strdup("handlersocket", MYF(0));
    }
    my_pthread_setspecific_ptr(THR_THD, thd);
    DBG_THR(fprintf(stderr, "HNDSOCK x0 %p\n", thd));
  }
  {
    pthread_mutex_lock(&LOCK_thread_count);
    thd->thread_id = thread_id++;
    threads.append(thd);
    ++thread_count;
    pthread_mutex_unlock(&LOCK_thread_count);
  }

  DBG_THR(fprintf(stderr, "HNDSOCK init thread wsts\n"));
  wait_server_to_start(thd, shutdown_flag);
  DBG_THR(fprintf(stderr, "HNDSOCK init thread done\n"));

  thd_proc_info(thd, &info_message_buf[0]);
  set_thread_message("hs:listening");
  DBG_THR(fprintf(stderr, "HNDSOCK x1 %p\n", thd));

  #if MYSQL_VERSION_ID >= 50505
  mdl_request = MDL_request::create(MDL_key::TABLE, "", "", for_write_flag ?
                  MDL_SHARED_WRITE : MDL_SHARED_READ, thd->mem_root);
  #endif

  lex_start(thd);

  user_lock.reset(new expr_user_lock(thd, user_level_lock_timeout));
}

int
dbcontext::set_thread_message(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  const int n = vsnprintf(&info_message_buf[0], info_message_buf.size(),
    fmt, ap);
  va_end(ap);
  return n;
}

void
dbcontext::term_thread()
{
  DBG_THR(fprintf(stderr, "HNDSOCK thread end %p\n", thd));
  unlock_tables_if();
  my_pthread_setspecific_ptr(THR_THD, 0);
  {
    pthread_mutex_lock(&LOCK_thread_count);
    delete thd;
    thd = 0;
    --thread_count;
    pthread_mutex_unlock(&LOCK_thread_count);
    my_thread_end();
  }
}

bool
dbcontext::check_alive()
{
  pthread_mutex_lock(&thd->mysys_var->mutex);
  THD::killed_state st = thd->killed;
  pthread_mutex_unlock(&thd->mysys_var->mutex);
  DBG_SHUT(fprintf(stderr, "chk HNDSOCK kst %p %p %d %zu\n", thd, &thd->killed,
    (int)st, sizeof(*thd)));
  if (st != THD::NOT_KILLED) {
    DBG_SHUT(fprintf(stderr, "chk HNDSOCK kst %d break\n", (int)st));
    return false;
  }
  return true;
}

void
dbcontext::lock_tables_if()
{
  if (lock_failed) {
    return;
  }
  if (for_write_flag && !user_level_lock_locked) {
    if (user_lock->get_lock()) {
      user_level_lock_locked = true;
    } else {
      lock_failed = true;
      return;
    }
  }
  if (lock == 0) {
    const size_t num_max = table_vec.size();
    TABLE *tables[num_max ? num_max : 1]; /* GNU */
    size_t num_open = 0;
    for (size_t i = 0; i < num_max; ++i) {
      if (table_vec[i].refcount > 0) {
    tables[num_open++] = table_vec[i].table;
      }
    }
    #if MYSQL_VERSION_ID >= 50505
    lock = thd->lock = mysql_lock_tables(thd, &tables[0], num_open, 0);
    #else
    bool need_reopen= false;
    lock = thd->lock = mysql_lock_tables(thd, &tables[0], num_open,
      MYSQL_LOCK_NOTIFY_IF_NEED_REOPEN, &need_reopen);
    #endif
    statistic_increment(lock_tables_count, &LOCK_status);
    thd_proc_info(thd, &info_message_buf[0]);
    DENA_VERBOSE(100, fprintf(stderr, "HNDSOCK lock tables %p %p %zu %zu\n",
      thd, lock, num_max, num_open));
    if (lock == 0) {
      lock_failed = true;
      DENA_VERBOSE(10, fprintf(stderr, "HNDSOCK failed to lock tables %p\n",
    thd));
    }
    if (for_write_flag) {
      #if MYSQL_VERSION_ID >= 50505
      thd->set_current_stmt_binlog_format_row();
      #else
      thd->current_stmt_binlog_row_based = 1;
      #endif
    }
  }
  DBG_LOCK(fprintf(stderr, "HNDSOCK tblnum=%d\n", (int)tblnum));
}

void
dbcontext::unlock_tables_if()
{
  if (lock != 0) {
    DENA_VERBOSE(100, fprintf(stderr, "HNDSOCK unlock tables\n"));
    if (for_write_flag) {
      bool suc = true;
      #if MYSQL_VERSION_ID >= 50505
      suc = (trans_commit_stmt(thd) == 0);
      #else
      suc = (ha_autocommit_or_rollback(thd, 0) == 0);
      #endif
      if (!suc) {
    commit_error = true;
    DENA_VERBOSE(10, fprintf(stderr,
      "HNDSOCK unlock tables: commit failed\n"));
      }
    }
    mysql_unlock_tables(thd, lock);
    lock = thd->lock = 0;
    statistic_increment(unlock_tables_count, &LOCK_status);
  }
  if (user_level_lock_locked) {
    if (user_lock->release_lock()) {
      user_level_lock_locked = false;
    }
  }
}

bool
dbcontext::get_commit_error()
{
  return commit_error;
}

void
dbcontext::clear_error()
{
  lock_failed = false;
  commit_error = false;
}

void
dbcontext::close_tables_if()
{
  unlock_tables_if();
  if (!table_vec.empty()) {
    DENA_VERBOSE(100, fprintf(stderr, "HNDSOCK close tables\n"));
    close_thread_tables(thd);
    statistic_increment(close_tables_count, &LOCK_status);
    table_vec.clear();
    table_map.clear();
  }
}

void
dbcontext::table_addref(size_t tbl_id)
{
  table_vec[tbl_id].refcount += 1;
  DBG_REFCNT(fprintf(stderr, "%p %zu %zu addref\n", this, tbl_id,
    table_vec[tbl_id].refcount));
}

void
dbcontext::table_release(size_t tbl_id)
{
  table_vec[tbl_id].refcount -= 1;
  DBG_REFCNT(fprintf(stderr, "%p %zu %zu release\n", this, tbl_id,
    table_vec[tbl_id].refcount));
}

void
dbcontext::resp_record(dbcallback_i& cb, TABLE *const table,
  const prep_stmt& pst)
{
  char rwpstr_buf[64];
  String rwpstr(rwpstr_buf, sizeof(rwpstr_buf), &my_charset_bin);
  const prep_stmt::retfields_type& rf = pst.get_retfields();
  const size_t n = rf.size();
  for (size_t i = 0; i < n; ++i) {
    uint32_t fn = rf[i];
    Field *const fld = table->field[fn];
    DBG_FLD(fprintf(stderr, "fld=%p %zu\n", fld, fn));
    if (fld->is_null()) {
      /* null */
      cb.dbcb_resp_entry(0, 0);
    } else {
      fld->val_str(&rwpstr, &rwpstr);
      const size_t len = rwpstr.length();
      if (len != 0) {
    /* non-empty */
    cb.dbcb_resp_entry(rwpstr.ptr(), rwpstr.length());
      } else {
    /* empty */
    static const char empty_str[] = "";
    cb.dbcb_resp_entry(empty_str, 0);
      }
    }
  }
}

void
dbcontext::dump_record(dbcallback_i& cb, TABLE *const table,
  const prep_stmt& pst)
{
  char rwpstr_buf[64];
  String rwpstr(rwpstr_buf, sizeof(rwpstr_buf), &my_charset_bin);
  const prep_stmt::retfields_type& rf = pst.get_retfields();
  const size_t n = rf.size();
  for (size_t i = 0; i < n; ++i) {
    uint32_t fn = rf[i];
    Field *const fld = table->field[fn];
    if (fld->is_null()) {
      /* null */
      cb.dbcb_resp_entry(0, 0);
      fprintf(stderr, "NULL");
    } else {
      fld->val_str(&rwpstr, &rwpstr);
      const std::string s(rwpstr.ptr(), rwpstr.length());
      fprintf(stderr, "[%s]", s.c_str());
    }
  }
  fprintf(stderr, "\n");
}

int
dbcontext::modify_record(dbcallback_i& cb, TABLE *const table,
  const prep_stmt& pst, const cmd_exec_args& args, char mod_op,
  size_t& success_count)
{
  if (mod_op == 'U') {
    /* update */
    handler *const hnd = table->file;
    uchar *const buf = table->record[0];
    store_record(table, record[1]);
    const prep_stmt::retfields_type& rf = pst.get_retfields();
    const size_t n = rf.size();
    for (size_t i = 0; i < n; ++i) {
      const string_ref& nv = args.uvals[i];
      uint32_t fn = rf[i];
      Field *const fld = table->field[fn];
      fld->store(nv.begin(), nv.size(), &my_charset_bin);
    }
    memset(buf, 0, table->s->null_bytes); /* TODO: allow NULL */
    const int r = hnd->ha_update_row(table->record[1], buf);
    if (r != 0 && r != HA_ERR_RECORD_IS_THE_SAME) {
      return r;
    }
    ++success_count;
  } else if (mod_op == 'D') {
    /* delete */
    handler *const hnd = table->file;
    const int r = hnd->ha_delete_row(table->record[0]);
    if (r != 0) {
      return r;
    }
    ++success_count;
  }
  return 0;
}

void
dbcontext::cmd_insert_internal(dbcallback_i& cb, const prep_stmt& pst,
  const string_ref *fvals, size_t fvalslen)
{
  if (!for_write_flag) {
    return cb.dbcb_resp_short(2, "readonly");
  }
  lock_tables_if();
  if (lock == 0) {
    return cb.dbcb_resp_short(2, "lock_tables");
  }
  if (pst.get_table_id() >= table_vec.size()) {
    return cb.dbcb_resp_short(2, "tblnum");
  }
  TABLE *const table = table_vec[pst.get_table_id()].table;
  handler *const hnd = table->file;
  uchar *const buf = table->record[0];
  empty_record(table);
  Field **fld = table->field;
  size_t i = 0;
  for (; *fld && i < fvalslen; ++fld, ++i) {
    (*fld)->store(fvals[i].begin(), fvals[i].size(), &my_charset_bin);
  }
  memset(buf, 0, table->s->null_bytes); /* TODO: allow NULL */
  const int r = hnd->ha_write_row(buf);
  return cb.dbcb_resp_short(r != 0 ? 1 : 0, "");
}

void
dbcontext::cmd_sql_internal(dbcallback_i& cb, const prep_stmt& pst,
  const string_ref *fvals, size_t fvalslen)
{
  if (fvalslen < 1) {
    return cb.dbcb_resp_short(2, "syntax");
  }
  return cb.dbcb_resp_short(2, "notimpl");
}

void
dbcontext::cmd_find_internal(dbcallback_i& cb, const prep_stmt& pst,
  ha_rkey_function find_flag, const cmd_exec_args& args)
{
  const bool debug_out = (verbose_level >= 100);
  const bool modify_op_flag = (args.mod_op.size() != 0);
  char mod_op = 0;
  if (modify_op_flag && !for_write_flag) {
    return cb.dbcb_resp_short(2, "readonly");
  }
  if (modify_op_flag) {
    mod_op = args.mod_op.begin()[0];
    if (mod_op != 'U' && mod_op != 'D') {
      return cb.dbcb_resp_short(2, "modop");
    }
  }
  lock_tables_if();
  if (lock == 0) {
    return cb.dbcb_resp_short(2, "lock_tables");
  }
  if (pst.get_table_id() >= table_vec.size()) {
    return cb.dbcb_resp_short(2, "tblnum");
  }
  TABLE *const table = table_vec[pst.get_table_id()].table;
  if (pst.get_idxnum() >= table->s->keys) {
    return cb.dbcb_resp_short(2, "idxnum");
  }
  KEY& kinfo = table->key_info[pst.get_idxnum()];
  if (args.kvalslen > kinfo.key_parts) {
    return cb.dbcb_resp_short(2, "kpnum");
  }
  uchar key_buf[kinfo.key_length]; /* GNU */
  size_t kplen_sum = 0;
  {
    DBG_KEY(fprintf(stderr, "SLOW\n"));
    for (size_t i = 0; i < args.kvalslen; ++i) {
      const KEY_PART_INFO & kpt = kinfo.key_part[i];
      const string_ref& kval = args.kvals[i];
      if (kval.begin() == 0) {
    kpt.field->set_null();
      } else {
    kpt.field->set_notnull();
      }
      kpt.field->store(kval.begin(), kval.size(), &my_charset_bin);
      kplen_sum += kpt.length;
    }
    key_copy(key_buf, table->record[0], &kinfo, kplen_sum);
  }
  table->read_set = &table->s->all_set;
  handler *const hnd = table->file;
  if (!for_write_flag) {
    hnd->init_table_handle_for_HANDLER();
  }
  hnd->ha_index_or_rnd_end();
  hnd->ha_index_init(pst.get_idxnum(), 1);
  #if 0
  statistic_increment(index_exec_count, &LOCK_status);
  #endif
  if (!modify_op_flag) {
    cb.dbcb_resp_begin(pst.get_retfields().size());
  } else {
    /* nothing to do */
  }
  const uint32_t limit = args.limit ? args.limit : 1;
  uint32_t skip = args.skip;
  size_t mod_success_count = 0;
  int r = 0;
  for (uint32_t i = 0; i < limit + skip; ++i) {
    if (i == 0) {
      const key_part_map kpm = (1U << args.kvalslen) - 1;
      r = hnd->index_read_map(table->record[0], key_buf, kpm, find_flag);
    } else {
      switch (find_flag) {
      case HA_READ_BEFORE_KEY:
      case HA_READ_KEY_OR_PREV:
    r = hnd->index_prev(table->record[0]);
    break;
      case HA_READ_AFTER_KEY:
      case HA_READ_KEY_OR_NEXT:
    r = hnd->index_next(table->record[0]);
    break;
      case HA_READ_KEY_EXACT:
    r = hnd->index_next_same(table->record[0], key_buf, kplen_sum);
    break;
      default:
    r = HA_ERR_END_OF_FILE; /* to finish the loop */
    break;
      }
    }
    if (debug_out) {
      fprintf(stderr, "r=%d\n", r);
      if (r == 0 || r == HA_ERR_RECORD_DELETED) {
    dump_record(cb, table, pst);
      }
    }
    if (r != 0) {
      /* no-count */
    } else if (skip > 0) {
      --skip;
    } else {
      if (!modify_op_flag) {
    resp_record(cb, table, pst);
      } else {
    r = modify_record(cb, table, pst, args, mod_op, mod_success_count);
      }
    }
    if (r != 0 && r != HA_ERR_RECORD_DELETED) {
      break;
    }
  }
  hnd->ha_index_or_rnd_end();
  if (r != 0 && r != HA_ERR_RECORD_DELETED && r != HA_ERR_KEY_NOT_FOUND &&
    r != HA_ERR_END_OF_FILE) {
    /* failed */
    if (!modify_op_flag) {
      /* revert dbcb_resp_begin() and dbcb_resp_entry() */
      cb.dbcb_resp_cancel();
    }
    cb.dbcb_resp_short_num(2, r);
  } else {
    /* succeeded */
    if (!modify_op_flag) {
      cb.dbcb_resp_end();
    } else {
      cb.dbcb_resp_short_num(0, mod_success_count);
    }
  }
}

void
dbcontext::cmd_open_index(dbcallback_i& cb, size_t pst_id, const char *dbn,
  const char *tbl, const char *idx, const char *retflds)
{
  unlock_tables_if();
  const table_name_type k = std::make_pair(std::string(dbn), std::string(tbl));
  const table_map_type::const_iterator iter = table_map.find(k);
  uint32_t tblnum = 0;
  if (iter != table_map.end()) {
    tblnum = iter->second;
    DBG_CMP(fprintf(stderr, "HNDSOCK k=%s tblnum=%d\n", k.c_str(),
      (int)tblnum));
  } else {
    TABLE_LIST tables;
    TABLE *table = 0;
    bool refresh = true;
    const thr_lock_type lock_type = for_write_flag ? TL_WRITE : TL_READ;
    #if MYSQL_VERSION_ID >= 50505
    tables.init_one_table(dbn, strlen(dbn), tbl, strlen(tbl), tbl,
      lock_type);
    tables.mdl_request = mdl_request;
    Open_table_context ot_act(thd, MYSQL_OPEN_REOPEN);
    if (!open_table(thd, &tables, thd->mem_root, &ot_act)) {
      table = tables.table;
    }
    #else
    tables.init_one_table(dbn, tbl, lock_type);
    table = open_table(thd, &tables, thd->mem_root, &refresh,
      OPEN_VIEW_NO_PARSE);
    #endif
    if (table == 0) {
      DENA_VERBOSE(10, fprintf(stderr,
    "HNDSOCK failed to open %p [%s] [%s] [%d]\n",
    thd, dbn, tbl, static_cast<int>(refresh)));
      return cb.dbcb_resp_short(2, "open_table");
    }
    statistic_increment(open_tables_count, &LOCK_status);
    table->reginfo.lock_type = lock_type;
    table->use_all_columns();
    tblnum = table_vec.size();
    tablevec_entry e;
    e.table = table;
    table_vec.push_back(e);
    table_map[k] = tblnum;
  }
  size_t idxnum = static_cast<size_t>(-1);
  if (idx[0] >= '0' && idx[0] <= '9') {
    /* numeric */
    TABLE *const table = table_vec[tblnum].table;
    idxnum = atoi(idx);
    if (idxnum >= table->s->keys) {
      return cb.dbcb_resp_short(2, "idxnum");
    }
  } else {
    const char *const idx_name_to_open = idx[0]  == '\0' ? "PRIMARY" : idx;
    TABLE *const table = table_vec[tblnum].table;
    for (uint i = 0; i < table->s->keys; ++i) {
      KEY& kinfo = table->key_info[i];
      if (strcmp(kinfo.name, idx_name_to_open) == 0) {
    idxnum = i;
    break;
      }
    }
  }
  if (idxnum == size_t(-1)) {
    return cb.dbcb_resp_short(2, "idxnum");
  }
  string_ref retflds_sr(retflds, strlen(retflds));
  std::vector<string_ref> fldnms;
  if (retflds_sr.size() != 0) {
    split(',', retflds_sr, fldnms);
  }
  prep_stmt::retfields_type rf;
  for (size_t i = 0; i < fldnms.size(); ++i) {
    TABLE *const table = table_vec[tblnum].table;
    Field **fld = 0;
    size_t j = 0;
    for (fld = table->field; *fld; ++fld, ++j) {
      DBG_FLD(fprintf(stderr, "f %s\n", (*fld)->field_name));
      string_ref fn((*fld)->field_name, strlen((*fld)->field_name));
      if (fn == fldnms[i]) {
    break;
      }
    }
    if (*fld == 0) {
      DBG_FLD(fprintf(stderr, "UNKNOWN FLD %s [%s]\n", retflds,
    std::string(fldnms[i].begin(), fldnms[i].size()).c_str()));
      return cb.dbcb_resp_short(2, "fld");
    }
    DBG_FLD(fprintf(stderr, "FLD %s %zu\n", (*fld)->field_name, j));
    rf.push_back(j);
  }
  prep_stmt p(this, tblnum, idxnum, rf);
  cb.dbcb_set_prep_stmt(pst_id, p);
  return cb.dbcb_resp_short(0, "");
}

enum db_write_op {
  db_write_op_none = 0,
  db_write_op_insert = 1,
  db_write_op_sql = 2,
};

void
dbcontext::cmd_exec_on_index(dbcallback_i& cb, const cmd_exec_args& args)
{
  const prep_stmt& p = *args.pst;
  if (p.get_table_id() == static_cast<size_t>(-1)) {
    return cb.dbcb_resp_short(2, "stmtnum");
  }
  ha_rkey_function find_flag = HA_READ_KEY_EXACT;
  db_write_op wrop = db_write_op_none;
  if (args.op.size() == 1) {
    switch (args.op.begin()[0]) {
    case '=':
      find_flag = HA_READ_KEY_EXACT;
      break;
    case '>':
      find_flag = HA_READ_AFTER_KEY;
      break;
    case '<':
      find_flag = HA_READ_BEFORE_KEY;
      break;
    case '+':
      wrop = db_write_op_insert;
      break;
    case 'S':
      wrop = db_write_op_sql;
      break;
    default:
      return cb.dbcb_resp_short(1, "op");
    }
  } else if (args.op.size() == 2 && args.op.begin()[1] == '=') {
    switch (args.op.begin()[0]) {
    case '>':
      find_flag = HA_READ_KEY_OR_NEXT;
      break;
    case '<':
      find_flag = HA_READ_KEY_OR_PREV;
      break;
    default:
      return cb.dbcb_resp_short(1, "op");
    }
  } else {
    return cb.dbcb_resp_short(1, "op");
  }
  if (args.kvalslen <= 0) {
    return cb.dbcb_resp_short(2, "klen");
  }
  switch (wrop) {
  case db_write_op_none:
    return cmd_find_internal(cb, p, find_flag, args);
  case db_write_op_insert:
    return cmd_insert_internal(cb, p, args.kvals, args.kvalslen);
  case db_write_op_sql:
    return cmd_sql_internal(cb, p, args.kvals, args.kvalslen);
  }
}

void
dbcontext::cmd_authorization(dbcallback_i& cb, int type, const char* key )
{
  std::string secret;
  if (for_write_flag && dbref->get_conf().get_str("secret_wr" , "" )!="") {
    secret = dbref->get_conf().get_str("secret_wr" , "" );
  } else {
    secret = dbref->get_conf().get_str("secret" , "" );
  }
  switch (type) {
  case 1:
    if (secret == key) {
      cb.set_authorization(true);
        cb.dbcb_resp_short(0 , "" );
    } else {
      /* authenticated failed */
      cb.set_authorization(false);
      cb.dbcb_resp_short(2, "authorization" );
    }
    break;
  default:
    cb.set_authorization(false);
    cb.dbcb_resp_short(2, "authtype");
    break;
  }
  return;
}

void
dbcontext::set_statistics(size_t num_conns, size_t num_active)
{
  thd_proc_info(thd, &info_message_buf[0]);
  if (for_write_flag) {
    set_thread_message("handlersocket: mode=wr, %zu conns, %zu active",
      num_conns, num_active);
  } else {
    set_thread_message("handlersocket: mode=rd, %zu conns, %zu active",
      num_conns, num_active);
  }
}

};

