//go:build unix

package main

// version — версия воркера.
const version = "1.0.0"

// jobRun — тип задачи «проверить сейчас».
const jobRun = "netprobe.run"

// manifest — самоописание воркера (sdk/spec §12): ключ настроек со схемой по пределам Spec,
// задача проверки.
var manifest = map[string]any{
	"version":     version,
	"description": "Связность узла с целями: потери и время ответа по ICMP или TCP",
	"configs": []any{map[string]any{
		"key":         configKey,
		"description": "Цели проверки и частота; не заданное — по умолчанию",
		"schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"targets": map[string]any{
					"type":     "array",
					"maxItems": maxTargets,
					"items": map[string]any{
						"type":     "object",
						"required": []string{"id", "host"},
						"properties": map[string]any{
							"id":     map[string]any{"type": "string", "minLength": 1, "maxLength": maxIDLen},
							"host":   map[string]any{"type": "string", "minLength": 1, "maxLength": maxHostLen},
							"port":   map[string]any{"type": "integer", "minimum": 0, "maximum": 65535},
							"method": map[string]any{"enum": []string{MethodICMP, MethodTCP}},
						},
					},
				},
				"intervalSec": map[string]any{"type": "integer", "minimum": 1, "maximum": maxIntervalSec},
				"count":       map[string]any{"type": "integer", "minimum": 1, "maximum": maxCount},
				"timeoutMs":   map[string]any{"type": "integer", "minimum": minTimeoutMs, "maximum": maxTimeoutMs},
			},
		},
	}},
	"jobs": []any{map[string]any{"type": jobRun,
		"description": "Проверка сейчас: цели из настроек или из data {targets?, count?, timeoutMs?}",
		"schema": map[string]any{"type": "object", "properties": map[string]any{
			"targets":   map[string]any{"type": "array", "maxItems": maxTargets},
			"count":     map[string]any{"type": "integer", "minimum": 1, "maximum": maxCount},
			"timeoutMs": map[string]any{"type": "integer", "minimum": minTimeoutMs, "maximum": maxTimeoutMs},
		}},
	}},
}
