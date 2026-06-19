#include "notify.h"
#include "server.h"
#include "utils.h"

#include <libwebsockets.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#define NOTIFY_BUF_LEN 4096

static char notify_buffer[NOTIFY_BUF_LEN];
static size_t notify_buffer_len = 0;
static bool notify_in_call = false;
static int notify_string_count = 0;
static char notify_app[256] = "";
static char notify_summary[512] = "";
static char notify_body[1024] = "";

void notify_add_client(struct lws *wsi) {
  struct client_node *node = xmalloc(sizeof(struct client_node));
  node->wsi = wsi;
  node->next = server->clients;
  server->clients = node;
}

void notify_remove_client(struct lws *wsi) {
  struct client_node **cur = &server->clients;
  while (*cur != NULL) {
    if ((*cur)->wsi == wsi) {
      struct client_node *tmp = *cur;
      *cur = (*cur)->next;
      free(tmp);
      return;
    }
    cur = &(*cur)->next;
  }
}

static void notify_broadcast(const char *app_name, const char *summary, const char *body) {
  struct client_node *cur = server->clients;
  while (cur != NULL) {
    struct lws *wsi = cur->wsi;
    struct pss_tty *pss = (struct pss_tty *)lws_wsi_user(wsi);
    if (pss->initialized && pss->process != NULL) {
      size_t json_len = strlen(app_name) + strlen(summary) + strlen(body) + 64;
      char *json = xmalloc(json_len);
      int n = snprintf(json, json_len,
        "{\"app\":\"%s\",\"summary\":\"%s\",\"body\":\"%s\"}",
        app_name, summary, body);
      if (n < 0) { free(json); cur = cur->next; continue; }
      size_t total = (size_t)n + 2;
      if (pss->notify_pending != NULL) free(pss->notify_pending);
      pss->notify_pending = xmalloc(total);
      pss->notify_pending[0] = NOTIFICATION;
      memcpy(pss->notify_pending + 1, json, (size_t)n);
      pss->notify_pending[total - 1] = '\0';
      free(json);
      lws_callback_on_writable(wsi);
    }
    cur = cur->next;
  }
}

static bool should_skip_notification(const char *app_name) {
  static const char *blocked[] = {"Chrome", "Chromium", "Firefox", "firefox", "Brave", "Edge", "Opera", "Vivaldi", "Epiphany", "Web", NULL};
  if (app_name[0] == '\0') return false;
  for (int i = 0; blocked[i] != NULL; i++) {
    if (strstr(app_name, blocked[i]) != NULL) return true;
  }
  return false;
}

static void parse_and_broadcast(const char *line) {
  if (strstr(line, "member=Notify") != NULL) {
    notify_in_call = true;
    notify_string_count = 0;
    notify_app[0] = '\0';
    notify_summary[0] = '\0';
    notify_body[0] = '\0';
    return;
  }

  if (!notify_in_call) return;

  if (line[0] == '\0') {
    notify_in_call = false;
    return;
  }

  if (strstr(line, "string ") == line) {
    const char *val_start = strchr(line, '"');
    if (val_start == NULL) return;
    val_start++;
    const char *val_end = strrchr(val_start, '"');
    if (val_end == NULL) return;
    size_t val_len = (size_t)(val_end - val_start);

    switch (notify_string_count) {
      case 0:
        snprintf(notify_app, sizeof(notify_app), "%.*s", (int)val_len, val_start);
        break;
      case 2:
        snprintf(notify_summary, sizeof(notify_summary), "%.*s", (int)val_len, val_start);
        break;
      case 3:
        snprintf(notify_body, sizeof(notify_body), "%.*s", (int)val_len, val_start);
        break;
    }
    notify_string_count++;
  }

  if (strstr(line, "int32 ") == line || strstr(line, "array ") == line) {
    notify_in_call = false;
    if (notify_string_count > 0 && (notify_summary[0] != '\0' || notify_body[0] != '\0')) {
      if (!should_skip_notification(notify_app)) {
        lwsl_notice("notification: app=%s summary=%s body=%s\n", notify_app, notify_summary, notify_body);
        notify_broadcast(notify_app, notify_summary, notify_body);
      } else {
        lwsl_info("notification from browser skipped: app=%s\n", notify_app);
      }
    }
    notify_string_count = 0;
    notify_app[0] = '\0';
    notify_summary[0] = '\0';
    notify_body[0] = '\0';
  }
}

static void on_alloc(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  (void)handle;
  buf->base = xmalloc(suggested_size);
  buf->len = suggested_size;
}

static void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  (void)stream;
  if (nread < 0) {
    if (buf->base != NULL) free(buf->base);
    return;
  }
  if (nread > 0) {
    for (ssize_t i = 0; i < nread; i++) {
      char c = buf->base[i];
      bool line_end = (c == '\n');
      if (c == '\r') continue;
      if (line_end) {
        notify_buffer[notify_buffer_len] = '\0';
        char *trimmed = notify_buffer;
        while (*trimmed == ' ' || *trimmed == '\t') trimmed++;
        parse_and_broadcast(trimmed);
        notify_buffer_len = 0;
      } else if (notify_buffer_len < NOTIFY_BUF_LEN - 1) {
        notify_buffer[notify_buffer_len++] = c;
      }
    }
  }
  if (buf->base != NULL) free(buf->base);
}

static void notify_on_exit(uv_process_t *proc, int64_t exit_status, int term_signal) {
  lwsl_warn("dbus-monitor exited with status %" PRId64 ", signal %d\n", exit_status, term_signal);
  uv_close((uv_handle_t *)&server->notify_pipe, NULL);
  uv_close((uv_handle_t *)proc, NULL);
  server->notify_active = false;
}

bool notify_start(uv_loop_t *loop) {
  if (server->notify_active) return true;

  uv_pipe_init(loop, &server->notify_pipe, 0);

  uv_process_options_t options;
  memset(&options, 0, sizeof(options));

  char *args[4];
  args[0] = "dbus-monitor";
  args[1] = "--session";
  args[2] = "interface='org.freedesktop.Notifications',member='Notify'";
  args[3] = NULL;

  options.file = "dbus-monitor";
  options.args = args;
  options.exit_cb = notify_on_exit;
  options.flags = 0;

  uv_stdio_container_t stdio[3];
  stdio[0].flags = UV_IGNORE;
  stdio[1].flags = (uv_stdio_flags)(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
  stdio[1].data.stream = (uv_stream_t *)&server->notify_pipe;
  stdio[2].flags = UV_IGNORE;
  options.stdio = stdio;
  options.stdio_count = 3;

  int r = uv_spawn(loop, &server->notify_proc, &options);
  if (r < 0) {
    lwsl_err("failed to spawn dbus-monitor: %s\n", uv_strerror(r));
    uv_close((uv_handle_t *)&server->notify_pipe, NULL);
    return false;
  }

  uv_read_start((uv_stream_t *)&server->notify_pipe, on_alloc, on_read);
  server->notify_active = true;
  lwsl_notice("notification monitor started (dbus-monitor)\n");
  return true;
}

void notify_stop(void) {
  if (server->notify_active) {
    uv_process_kill(&server->notify_proc, SIGTERM);
    server->notify_active = false;
  }

  struct client_node *cur = server->clients;
  while (cur != NULL) {
    struct client_node *tmp = cur;
    cur = cur->next;
    free(tmp);
  }
  server->clients = NULL;
}
