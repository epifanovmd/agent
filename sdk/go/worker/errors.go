package worker

import (
	"fmt"
	"regexp"
)

// JobFailed — провал задачи с кодом; Retryable: false — без повторов.
// Возвращается обработчиком задачи (см. Fail).
type JobFailed struct {
	Code      string
	Message   string
	Retryable bool
}

func (e *JobFailed) Error() string { return e.Code + ": " + e.Message }

// Fail — ошибка задачи с кодом (^[A-Z0-9_]+$) для job.fail.
func Fail(code, msg string, retryable bool) error {
	return &JobFailed{Code: code, Message: msg, Retryable: retryable}
}

// CommandFailed — команда завершилась ошибкой с кодом (см. CommandError).
type CommandFailed struct {
	Code    string
	Message string
}

func (e *CommandFailed) Error() string { return e.Code + ": " + e.Message }

// CommandError — ошибка команды с кодом (^[A-Z0-9_]+$) для cmd.done.
func CommandError(code, msg string) error {
	return &CommandFailed{Code: code, Message: msg}
}

// StateFailure — снимок не применён, но есть отчёт (см. StateFailed).
type StateFailure struct {
	Message string
	Report  any
}

func (e *StateFailure) Error() string { return e.Message }

// StateFailed — ошибка применения снимка вместе с отчётом: обработчик
// состояния возвращает (nil, StateFailed("порт занят", report)), SDK шлёт
// state.applied {ok: false, error, report}. Обычная ошибка — без report.
func StateFailed(msg string, report any) error {
	return &StateFailure{Message: msg, Report: report}
}

// Коды, которые выставляет SDK.
const (
	CodeWorkerError    = "WORKER_ERROR"
	CodeWorkerStopping = "WORKER_STOPPING"
	CodeCommandFailed  = "COMMAND_FAILED"
	CodeCommandUnknown = "COMMAND_UNKNOWN"
)

var codeRe = regexp.MustCompile(`^[A-Z0-9_]{1,64}$`)

// normalizeCode — код по схеме спецификации; негодный заменяется общим, исходный
// уходит в текст.
func normalizeCode(code, message, fallback string) (string, string) {
	if codeRe.MatchString(code) {
		return code, message
	}
	return fallback, fmt.Sprintf("%s: %s", code, message)
}

// truncate — не больше n рун (строка остаётся корректным UTF-8).
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	i := 0
	for pos := range s {
		if i == n {
			return s[:pos]
		}
		i++
	}
	return s
}
