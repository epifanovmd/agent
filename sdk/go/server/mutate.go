package server

import (
	"errors"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// mutateRetries — попыток условной записи, пока запись меняют другие.
const mutateRetries = 8

// errBusy — запись так и не сохранилась: её всё время меняли другие
// (STORE_CONFLICT).
var errBusy = &message.Error{Code: "STORE_CONFLICT", Message: "Запись одновременно меняют другие процессы: повторите"}

// mutate — чтение-изменение-запись с повтором: fn меняет свежую запись и
// говорит, писать ли (false — условие не выполнено, ничего не делать). Запись
// изменилась между чтением и записью — перечитать и повторить. fn может
// вызываться несколько раз: побочные действия — у вызывающего, по итогу
// (запись, written). Записи нет — ErrNotFound.
func mutate[T any](get func(string) (T, error), update func(T) (bool, error), id string,
	fn func(T) bool) (rec T, written bool, err error) {
	for range mutateRetries {
		if rec, err = get(id); err != nil {
			return rec, false, err
		}
		if !fn(rec) {
			return rec, false, nil
		}
		if written, err = update(rec); err != nil || written {
			return rec, written, err
		}
	}
	return rec, false, errBusy
}

// mutateAgent — изменить запись агента (mutate): только свои поля поверх
// свежей записи, остальное (revoked, подписки, ключи) не затирается.
func (a *Agents) mutateAgent(id string, fn func(*Agent) bool) (*Agent, bool, error) {
	agent, written, err := mutate(a.store.GetAgent, a.store.UpdateAgent, id, fn)
	if err != nil && !errors.Is(err, ErrNotFound) {
		a.log.Error("агент не сохранён", "agent", id, "err", err)
	}
	return agent, written, err
}

// mutateJob — изменить запись задачи (mutate).
func (a *Agents) mutateJob(id string, fn func(*Job) bool) (*Job, bool, error) {
	job, written, err := mutate(a.store.GetJob, a.store.UpdateJob, id, fn)
	if err != nil && !errors.Is(err, ErrNotFound) {
		a.log.Error("задача не сохранена", "job", id, "err", err)
	}
	return job, written, err
}

// mutateCommand — изменить запись команды (mutate).
func (a *Agents) mutateCommand(id string, fn func(*Command) bool) (*Command, bool, error) {
	cmd, written, err := mutate(a.store.GetCommand, a.store.UpdateCommand, id, fn)
	if err != nil && !errors.Is(err, ErrNotFound) {
		a.log.Error("команда не сохранена", "command", id, "err", err)
	}
	return cmd, written, err
}

// heldBy — задача выполняется у агента в этой попытке.
func heldBy(job *Job, agentID string, ref message.JobRef) bool {
	return job.Status == JobRunning && job.AgentID == agentID && job.Attempt == ref.Attempt
}

// updateHeld — изменить задачу, пока она за агентом в попытке ref (по свежей
// записи): fn меняет её и говорит, писать ли. held — задача за агентом.
func (a *Agents) updateHeld(agentID string, ref message.JobRef, fn func(*Job) bool) (job *Job, held, written bool, err error) {
	job, written, err = a.mutateJob(ref.JobID, func(j *Job) bool {
		if held = heldBy(j, agentID, ref); !held {
			return false
		}
		return fn(j)
	})
	if errors.Is(err, ErrNotFound) {
		return nil, false, false, nil
	}
	return job, held, written, err
}
