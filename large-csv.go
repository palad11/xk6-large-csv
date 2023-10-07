package xk6_large_csv

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/dop251/goja"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"io"
	"os"
	"strings"
	"sync"
)

// init is called by the Go runtime at application startup.
func init() {
	modules.Register("k6/x/large-csv", New())
}

type (
	// RootModule is the global module instance that will create module
	// instances for each VU.
	RootModule struct {
		comparator Compare
	}

	// ModuleInstance represents an instance of the JS module.
	ModuleInstance struct {
		// vu provides methods for accessing internal k6 objects for a VU
		vu modules.VU
		// comparator is the exported type
		comparator *Compare
	}
	// Compare is the type for our custom API.
	Compare struct {
		vu       modules.VU // provides methods for accessing internal k6 objects
		mu       sync.Mutex
		FilePath string
		File     *os.File
		Scanner  *bufio.Scanner
	}
)

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Instance = &ModuleInstance{}
	_ modules.Module   = &RootModule{}
)

// New returns a pointer to a new RootModule instance.
func New() *RootModule {
	return &RootModule{comparator: Compare{}}
}

// NewModuleInstance implements the modules.Module interface returning a new instance for each VU.
func (rm *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ModuleInstance{
		vu:         vu,
		comparator: &rm.comparator,
	}
}

// Exports implements the modules.Instance interface and returns the exported types for the JS module.
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{Named: map[string]interface{}{
		"getCsv": mi.GetCsv,
	}}
}

// Close closes the file
func (c *Compare) Close() {
	c.File.Close()
}

// GetLine reads line and checks for EOF, then split by sep and return array with values
func (c *Compare) GetLine(sep string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Scanner.Scan() {
		line := c.Scanner.Text()
		if err := c.Scanner.Err(); err != nil {
			if err != io.EOF {
				common.Throw(c.vu.Runtime(), errors.New("file corrupted"))
			}
		}
		variables := strings.Split(line, sep)
		return variables
	} else {
		// Если достигнут конец файла, перейдем в начало файла
		_, err := c.File.Seek(0, 0)
		if err != nil {
			panic(err)
		}
		c.Scanner = bufio.NewScanner(c.File)
		if c.Scanner.Scan() {
			line := c.Scanner.Text()
			if err := c.Scanner.Err(); err != nil {
				if err != io.EOF {
					common.Throw(c.vu.Runtime(), errors.New("file corrupter"))
				}
			}
			variables := strings.Split(line, sep)
			return variables
		} else {
			common.Throw(c.vu.Runtime(), errors.New("file is empty"))
			return []string{"file is broken"}
		}
	}

}

// GetCsv reads the csv file and returns a csv instance.
func (mi *ModuleInstance) GetCsv(call goja.ConstructorCall) *goja.Object {
	rt := mi.vu.Runtime()
	mi.comparator.FilePath = call.Argument(0).String()

	fileStats, err := os.Stat(mi.comparator.FilePath)
	if err != nil {
		common.Throw(rt, errors.New("Cant open the file"))
		panic(err)
	}
	if fileStats.Size() == 0 {
		common.Throw(rt, errors.New(fmt.Sprintf("File %s is empty", mi.comparator.FilePath)))
		panic(err)
	}

	file, err := os.Open(mi.comparator.FilePath)
	if err != nil {
		common.Throw(rt, errors.New("Cant open the file"))
		panic(err)
	}
	mi.comparator.File = file
	scanner := bufio.NewScanner(file)
	mi.comparator.Scanner = scanner
	return rt.ToValue(mi.comparator).ToObject(rt)
}
