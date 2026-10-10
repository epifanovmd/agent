package releases

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// CheckFile — итог последней проверки новой версии в каталоге данных агента.
const CheckFile = "update-check.json"

// Check — итог проверки: последняя версия в каталоге сборок и когда проверено.
type Check struct {
	Latest    string `json:"latest"`
	CheckedAt int64  `json:"checkedAt"`
}

// Newer — последняя версия новее version (агент без версии — dev — не сравнивается).
func (c Check) Newer(version string) bool {
	return c.Latest != "" && version != "dev" && message.CompareVersions(c.Latest, version) > 0
}

// Fresh — проверка моложе every.
func (c Check) Fresh(every time.Duration, now time.Time) bool {
	return c.CheckedAt > 0 && now.Sub(time.UnixMilli(c.CheckedAt)) < every
}

// Info — для hello и status: nil, если новее version нет.
func (c Check) Info(version string) *message.AgentUpdateInfo {
	if !c.Newer(version) {
		return nil
	}
	return &message.AgentUpdateInfo{Latest: c.Latest, CheckedAt: c.CheckedAt}
}

// Latest — последняя версия агента в каталоге сборок base ("" — релизы на GitHub).
func Latest(ctx context.Context, client *http.Client, base string, now time.Time) (Check, error) {
	m, err := Manifest(ctx, client, LatestDir(base))
	if err != nil {
		return Check{}, err
	}
	return Check{Latest: m.Version, CheckedAt: now.UnixMilli()}, nil
}

// ReadCheck — итог последней проверки из каталога данных (нет — пустой).
func ReadCheck(dataDir string) Check {
	var c Check
	if raw, err := os.ReadFile(filepath.Join(dataDir, CheckFile)); err == nil {
		_ = json.Unmarshal(raw, &c)
	}
	return c
}

// WriteCheck — сохранить итог проверки (нет прав на каталог — не ошибка для команд).
func WriteCheck(dataDir string, c Check) error {
	raw, _ := json.Marshal(c)
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dataDir, CheckFile)
	tmp := path + ".new"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
