//go:build unix

package integration

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/test/testserver"
)

// Запрос воркера к серверу (§12): POST /requests на сокете агента → request → onWorkerRequest
// сервера → request.result → ответ воркеру. Тип не из манифеста — 400 REQUEST_UNDECLARED без
// запроса к серверу; отказ сервера — 422 с его кодом; срок — 504 TIMEOUT; без связи — сразу
// 503 AGENT_OFFLINE.
func TestWorkerRequests(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	n := s.start(s.config(s.worker("w")))
	a := s.running("w")

	ask := func(query string) (string, message.ErrorInfo, json.RawMessage) {
		t.Helper()
		res := s.fetch(a.ID, "w", "/ask?"+query, testserver.FetchInit{Method: "POST"})
		var e message.ErrorInfo
		var reply message.RequestReply
		_ = json.Unmarshal([]byte(res.Body), &e)
		_ = json.Unmarshal([]byte(res.Body), &reply)
		return res.Headers["x-agent-status"], e, reply.Data
	}

	status, _, data := ask("type=example.ask&timeoutMs=5000")
	var got struct {
		Worker, Agent string
		Echo          map[string]int
	}
	if status != "200" || json.Unmarshal(data, &got) != nil || got.Worker != "w" || got.Agent != agentName || got.Echo["n"] != 1 {
		t.Fatalf("запрос: %s %s", status, data)
	}
	if status, e, _ := ask("type=example.deny"); status != "422" || e.Code != "EXAMPLE_DENIED" {
		t.Fatalf("отказ сервера: %s %+v", status, e)
	}
	if status, e, _ := ask("type=example.other"); status != "400" || e.Code != message.CodeRequestUndeclared {
		t.Fatalf("необъявленный запрос: %s %+v", status, e)
	}
	if status, e, _ := ask("type=example.slow&timeoutMs=300"); status != "504" || e.Code != message.CodeTimeout {
		t.Fatalf("срок: %s %+v", status, e)
	}
	var seen []struct{ Type string }
	s.server.Must("workerRequests", &seen, a.ID)
	if len(seen) != 3 || seen[0].Type != "example.ask" || seen[2].Type != "example.slow" {
		t.Fatalf("запросы на сервере: %+v", seen)
	}

	// Без связи — сразу AGENT_OFFLINE.
	s.setDown(true)
	eventually(t, "агент без связи", func() bool {
		code, body := n.direct("w", http.MethodPost, "/ask?type=example.ask")
		var e message.ErrorInfo
		_ = json.Unmarshal([]byte(body), &e)
		return code == http.StatusOK && e.Code == message.CodeAgentOffline
	})
	s.setDown(false)
}
