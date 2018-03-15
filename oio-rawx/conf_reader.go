package main

import (
    "bufio"
    "os"
    "strings"
)

// ReadConfig -- fetch options from conf file
func ReadConfig(conf string) (map[string]string, error) {
    loadedOpts := map[string]string{
            "Listen": "addr",
            "grid_filerepos": "filerepos",
            "grid_namespace": "ns",
            "grid_docroot": "filerepo",
    }
    var opts = make(map[string]string)
    f, err := os.OpenFile(conf, os.O_RDONLY, os.ModePerm)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    sc := bufio.NewScanner(f)
    for sc.Scan() {
        fields := strings.Fields(sc.Text())
        if len(fields) > 1 {
            if v, found := loadedOpts[fields[0]]; found {
                opts[v] = fields[1]
            }
        }
    }
    if err := sc.Err(); err != nil {
        return nil, err
    }

    return opts, nil
}
