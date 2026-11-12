"""本地 HTTP fixture：模拟茶叶后端的真实响应契约。

- 路径 /api/farmers、/api/tea-plots、/api/purchase-orders 返回
  {"ok": true, "message": "OK", "data": [...]}
- 支持注入故障：连续 N 次 503/429/超时断连/非法 JSON/ok=false/结构错误
- 支持 Retry-After 头、慢响应
- 记录请求信息（含收到的认证头名，用于断言头被传递，但测试不断言其值落日志）

仅用于测试，标准库 http.server 实现。
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit


def make_farmer(fid, name="陈兴国", phone="13870010001", group="云岭村一组",
                created="2026-10-20T08:00:00", updated=None):
    data = {"id": fid, "name": name, "phone": phone, "villageGroup": group,
            "createdAt": created, "updatedAt": updated}
    return data


def make_plot(pid, farmer_id, name="云岭东坡", area="12.50", altitude=980,
              season="春季", created="2026-10-20T09:00:00", updated=None):
    return {"id": pid, "farmerId": farmer_id, "plotName": name, "areaMu": area,
            "altitudeM": altitude, "pickingSeason": season,
            "createdAt": created, "updatedAt": updated}


def make_purchase(pid, order_no, farmer_id, plot_id, weight="286.50",
                  moisture="74.20", picked="2026-11-01T06:20:00",
                  purchaser="刘海", date="2026-11-01",
                  created="2026-11-01T08:00:00", updated=None):
    return {"id": pid, "orderNo": order_no, "farmerId": farmer_id,
            "plotId": plot_id, "weightKg": weight, "moisturePercent": moisture,
            "pickedAt": picked, "purchaser": purchaser, "purchaseDate": date,
            "createdAt": created, "updatedAt": updated}


def healthy_dataset():
    """与 schema.sql 种子数据同构的一套健康数据。"""
    farmers = [
        make_farmer(1, "陈兴国", "13870010001", "云岭村一组"),
        make_farmer(2, "周明秀", "13870010002", "云岭村三组"),
        make_farmer(3, "王守田", "13870010003", "青峰村二组"),
        make_farmer(4, "李春梅", "13870010004", "青峰村五组"),
        make_farmer(5, "赵德安", "13870010005", "石坎村四组"),
    ]
    plots = [
        make_plot(1, 1, "云岭东坡", "12.50", 980, "春季"),
        make_plot(2, 2, "老茶湾", "8.20", 1120, "春季"),
        make_plot(3, 3, "青峰南坳", "15.80", 860, "夏季"),
        make_plot(4, 4, "竹林坡", "9.60", 1050, "秋季"),
        make_plot(5, 5, "石坎高山园", "18.40", 1260, "春季"),
    ]
    purchases = [
        make_purchase(1, "CG20261101001", 1, 1, "286.50", "74.20",
                      "2026-11-01T06:20:00", "刘海", "2026-11-01"),
        make_purchase(2, "CG20261101002", 2, 2, "198.00", "73.80",
                      "2026-11-01T06:45:00", "刘海", "2026-11-01"),
        make_purchase(3, "CG20261102001", 3, 3, "315.20", "75.10",
                      "2026-11-02T06:10:00", "孙强", "2026-11-02"),
        make_purchase(4, "CG20261102002", 4, 4, "226.80", "72.90",
                      "2026-11-02T06:35:00", "孙强", "2026-11-02"),
        make_purchase(5, "CG20261103001", 5, 5, "342.60", "74.60",
                      "2026-11-03T06:15:00", "刘海", "2026-11-03"),
    ]
    return {"farmers": farmers, "tea_plots": plots, "purchase_orders": purchases}


PATH_BY_RESOURCE = {
    "farmers": "/api/farmers",
    "tea_plots": "/api/tea-plots",
    "purchase_orders": "/api/purchase-orders",
}


class FixtureState:
    def __init__(self, dataset):
        self.dataset = dataset
        self.lock = threading.Lock()
        # resource -> {"failures": [(kind, kwargs), ...], "calls": n}
        self.config = {r: {"failures": [], "calls": 0, "received_headers": []}
                       for r in PATH_BY_RESOURCE}
        self.slow = {}  # resource -> seconds

    def set_failures(self, resource, kinds):
        self.config[resource]["failures"] = list(kinds)

    def reset_calls(self):
        for cfg in self.config.values():
            cfg["calls"] = 0


def _build_handler(state: FixtureState):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # 静默，避免污染测试输出
            pass

        def do_GET(self):  # noqa: N802
            path = urlsplit(self.path).path
            resource = None
            for key, prefix in PATH_BY_RESOURCE.items():
                if path == prefix:
                    resource = key
                    break
            if resource is None:
                self._send_json(404, {"ok": False, "message": "not found", "data": None})
                return
            with state.lock:
                cfg = state.config[resource]
                cfg["calls"] += 1
                cfg["received_headers"].append({k: v for k, v in self.headers.items()})
                failure = cfg["failures"].pop(0) if cfg["failures"] else None
                slow = state.slow.get(resource, 0)
            if slow:
                time.sleep(slow)
            if failure is not None:
                self._apply_failure(failure)
                return
            body = {"ok": True, "message": "OK", "data": state.dataset[resource]}
            self._send_json(200, body)

        def _apply_failure(self, failure):
            kind = failure[0] if isinstance(failure, tuple) else failure
            kwargs = failure[1] if isinstance(failure, tuple) and len(failure) > 1 else {}
            if kind == "503":
                self._send_json(503, {"ok": False, "message": "服务暂不可用",
                                      "data": None},
                                headers=kwargs.get("headers"))
            elif kind == "500":
                self._send_json(500, {"ok": False,
                                      "message": "服务异常：模拟故障", "data": None})
            elif kind == "429":
                self._send_json(429, {"ok": False, "message": "请求过于频繁",
                                      "data": None},
                                headers=kwargs.get("headers"))
            elif kind == "400-business":
                self._send_json(400, {"ok": False,
                                      "message": "模拟业务失败：参数不合法", "data": None})
            elif kind == "404":
                self._send_json(404, {"ok": False, "message": "资源不存在",
                                      "data": None})
            elif kind == "bad-json":
                payload = b"{this is not valid json"
                self.send_response(200)
                self.send_header("Content-Type", "application/json;charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            elif kind == "bad-structure":
                self._send_json(200, ["unexpected", "array"])
            elif kind == "ok-false-200":
                self._send_json(200, {"ok": False, "message": "模拟 ok=false",
                                      "data": None})
            elif kind == "data-not-array":
                self._send_json(200, {"ok": True, "message": "OK",
                                      "data": {"unexpected": "object"}})
            elif kind == "drop-connection":
                # 直接关闭底层连接模拟连接重置/读取失败
                self.close_connection = True
                self.wfile.flush()
                self.connection.close()
            else:
                self._send_json(500, {"ok": False, "message": f"未知故障 {kind}",
                                      "data": None})

        def _send_json(self, status, payload, headers=None):
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json;charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            if headers:
                for key, value in headers.items():
                    self.send_header(key, value)
            self.end_headers()
            self.wfile.write(data)

    return Handler


class FixtureServer:
    def __init__(self, dataset=None):
        self.state = FixtureState(dataset or healthy_dataset())
        self.httpd = HTTPServer(("127.0.0.1", 0), _build_handler(self.state))
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def base_url(self):
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
