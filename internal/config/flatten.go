package config

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Flatten — цепочка файлов path одним файлом для агента на узле (agent install
// из папки агента): без extends и envFiles; ${ИМЯ} не подставляются — их
// значения установка кладёт в agent.env, служба передаёт их агенту; dataDir —
// каталог узла dataDir; воркер из папки (path) становится воркером со сборкой
// (release: true): установка кладёт сборку в <dataDir>/workers/<имя>; instance
// и install убираются — они нужны только установке. Комментарии файлов
// сохраняются.
func Flatten(path, dataDir, header string) ([]byte, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	chain, problems := readChain(abs)
	if len(problems) > 0 {
		return nil, problemsError(problems)
	}
	var root *yaml.Node
	for _, l := range chain {
		var doc yaml.Node
		if err := yaml.Unmarshal([]byte(maskVars(l.raw)), &doc); err != nil {
			return nil, fmt.Errorf("%s: %w", l.file, err)
		}
		m := mappingOf(&doc)
		if m == nil {
			return nil, fmt.Errorf("%s: %s", l.file, notMapping)
		}
		dropKeys(m, "extends", "envFiles", "instance", "install")
		for _, key := range []string{"caFile", "certFile", "keyFile"} {
			if v := mapValue(mapValue(m, "server"), key); v != nil && v.Value != "" && !filepath.IsAbs(v.Value) && !strings.HasPrefix(v.Value, "__agentvar__") {
				return nil, fmt.Errorf("%s: server.%s %q — путь от папки агента на узле не годится: укажите путь на узле или --ca-file", l.file, key, v.Value)
			}
		}
		if root == nil {
			root = m
		} else {
			mergeMap(root, m, true)
		}
	}
	if root == nil {
		return nil, errors.New("нет файла настроек")
	}
	setScalar(root, "dataDir", dataDir)
	if workers := mapValue(root, "workers"); workers != nil && workers.Kind == yaml.SequenceNode {
		for _, w := range workers.Content {
			p := mapValue(w, "path")
			if p == nil || p.Value == "" {
				continue
			}
			if mapValue(w, "name") == nil {
				setScalar(w, "name", filepath.Base(p.Value))
			}
			if name := mapValue(w, "name"); p.LineComment != "" && name.LineComment == "" {
				name.LineComment = p.LineComment
			}
			dropKeys(w, "path", "dir")
			setScalar(w, "release", "true")
			mapValue(w, "release").Tag = "!!bool"
		}
	}
	root.HeadComment = header
	var b bytes.Buffer
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return []byte(unmaskVars(b.String())), nil
}

// setScalar — ключ key словаря m со строковым значением value (есть — заменить).
func setScalar(m *yaml.Node, key, value string) {
	if i := mapIndex(m, key); i >= 0 {
		v := m.Content[i+1]
		v.Kind, v.Tag, v.Value, v.Content, v.Style = yaml.ScalarNode, "!!str", value, nil, 0
		return
	}
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value})
}
