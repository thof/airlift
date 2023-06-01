package main

import (
	"bufio"
	"flag"
	"fmt"
	"golang.org/x/sys/unix"
	proc "launcher2/process"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Options struct {
	Verbose          bool
	InstallPath      string
	LauncherConfig   string
	EtcDir           string
	NodeConfig       string
	JvmConfig        string
	ConfigPath       string
	LogLevels        string
	LogLevelsSet     bool
	JvmOptions       []string
	DataDir          string
	PidFile          string
	LauncherLog      string
	ServerLog        string
	SystemProperties map[string]string
}

type arrayFlags []string

var COMMANDS = []string{"run", "start", "stop", "restart", "kill", "status"}

var verboseOpt bool
var etcDirOpt string
var launcherConfigOpt string
var nodeConfigOpt string
var jvmConfigOpt string
var configOpt string
var logLevelsFileOpt string
var dataDirOpt string
var pidFileOpt string
var launcherLogFileOpt string
var serverLogFileOpt string
var jvmOpt = arrayFlags{}
var systemPropertiesOpt arrayFlags

func init() {
	commands := "Commands: " + strings.Join(COMMANDS, ", ")
	flag.Usage = func() {
		_, _ = fmt.Fprintf(os.Stderr, "usage: %s [options] command\n%s\n", os.Args[0], commands)
		flag.PrintDefaults()
	}
	const usage = "Run verbosely"
	flag.BoolVar(&verboseOpt, "verbose", false, usage)
	flag.BoolVar(&verboseOpt, "v", false, usage)
	flag.StringVar(&etcDirOpt, "etc-dir", "", "Defaults to INSTALL_PATH/etc")
	flag.StringVar(&launcherConfigOpt, "launcher-config", "", "Defaults to INSTALL_PATH/bin/launcher.properties")
	flag.StringVar(&nodeConfigOpt, "node-config", "", "Defaults to ETC_DIR/node.properties")
	flag.StringVar(&jvmConfigOpt, "jvm-config", "", "Defaults to ETC_DIR/jvm.configOpt")
	flag.StringVar(&configOpt, "config", "", "Defaults to ETC_DIR/configOpt.properties")
	flag.StringVar(&logLevelsFileOpt, "log-levels-file", "", "Defaults to ETC_DIR/log.properties")
	flag.StringVar(&dataDirOpt, "data-dir", "", "Defaults to INSTALL_PATH")
	flag.StringVar(&pidFileOpt, "pid-file", "", "Defaults to DATA_DIR/var/run/launcher.pid")
	flag.StringVar(&launcherLogFileOpt, "launcher-log-file", "", "Defaults to DATA_DIR/var/log/launcher.log (only in daemon mode)")
	flag.StringVar(&serverLogFileOpt, "server-log-file", "", "Defaults to DATA_DIR/var/log/server.log (only in daemon mode)")
	flag.Var(&jvmOpt, "J", "Set a JVM option")
	flag.Var(&systemPropertiesOpt, "D", "Set a Java system property")
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func insert(a []string, index int, value string) []string {
	if len(a) == index { // nil or empty slice or after last element
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...) // index < len(a)
	a[index] = value
	return a
}

func transformArgs(args []string) []string {
	for i := range args {
		if strings.HasPrefix(args[i], "-D") || strings.HasPrefix(args[i], "-J") {
			key := args[i][:2]
			value := args[i][2:]
			args[i] = key
			args = insert(args, i+1, value)
		}
	}
	return args
}

func getPath(pathPrimary string, pathSecondary string, installPath string) string {
	var pathOut = pathPrimary
	if pathPrimary == "" {
		pathOut = filepath.Join(installPath, pathSecondary)
	}
	pathOut, _ = filepath.Abs(pathOut)
	return pathOut
}

func (i *arrayFlags) String() string {
	return "options"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, strings.TrimSpace(value))
	return nil
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// findInstallPath finds canonical parent of bin/launcher.py
func findInstallPath() string {
	currentFile, _ := os.Executable()
	if !(filepath.Base(currentFile) == "launcher2" || filepath.Base(currentFile) == "main.go") {
		panic(fmt.Sprintf("Expected file '%s' to be 'launcher2' not '%s'", currentFile, filepath.Base(currentFile)))
	}
	var p, _ = filepath.Abs(filepath.Dir(currentFile))
	if !(filepath.Base(p) == "bin" || filepath.Base(p) == "launcher2") {
		panic(fmt.Sprintf("Expected file '%s' directory to be 'bin' not '%s", currentFile, filepath.Base(p)))
	}
	return filepath.Dir(p)
}

// makedirs creates directory and all intermediate ones
func makedirs(p string) {
	err := os.MkdirAll(p, os.ModePerm)
	if err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
}

// loadProperties loads key/value pairs from a file
func loadProperties(f string) map[string]string {
	properties := map[string]string{}
	for _, line := range loadLines(f) {
		var kv = strings.SplitN(line, "=", 2)
		properties[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return properties
}

// loadLines loads lines from a file, ignoring blank or comment lines
func loadLines(f string) []string {
	readFile, err := os.Open(f)
	if err != nil {
		panic(err)
	}
	defer func(readFile *os.File) {
		_ = readFile.Close()
	}(readFile)

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	var fileLines []string
	for fileScanner.Scan() {
		var line = strings.TrimSpace(fileScanner.Text())
		if len(line) > 0 && !strings.HasPrefix(line, "#") {
			fileLines = append(fileLines, line)
		}
	}
	if err = fileScanner.Err(); err != nil {
		panic(err)
	}
	return fileLines
}

// redirectStdinToDevnull redirects stdin to /dev/null
func redirectStdinToDevnull() {
	fd, _ := os.Open(os.DevNull)
	_ = unix.Dup2(int(fd.Fd()), int(os.Stdin.Fd()))
	_ = fd.Close()
}

// openAppend opens a raw file descriptor in append mode
func openAppend(f string) *os.File {
	file, err := os.OpenFile(f, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	return file
}

// redirectOutput redirects stdout and stderr to a file descriptor
func redirectOutput(f *os.File) {
	_ = unix.Dup2(int(f.Fd()), int(os.Stdout.Fd()))
}

// symlinkExists checks if symlink exists and raise if another type of file exists
func symlinkExists(p string) bool {
	st, err := os.Lstat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		panic(err)
	}
	if st.Mode()&os.ModeSymlink == 0 {
		panic(fmt.Sprintf("Path exists and is not a symlink: %s", p))
	}
	return true
}

// createSymlink creates a symlink, removing the target first if it is a symlink
func createSymlink(source string, target string) {
	if symlinkExists(target) {
		err := os.Remove(target)
		if err != nil {
			panic(err)
		}
	}
	if _, err := os.Stat(source); err == nil {
		err = os.Symlink(source, target)
		if err != nil {
			panic(err)
		}
	}
}

// createAppSymlinks symlinks the 'etc' and 'plugin' directory into the data directory.
// This is needed to support programs that reference 'etc/xyz' from within
// their config files: log.levels-file=etc/log.properties
func createAppSymlinks(options Options) {
	if options.EtcDir != path.Join(options.DataDir, "etc") {
		createSymlink(options.EtcDir, path.Join(options.DataDir, "etc"))
	}
	if options.InstallPath != options.DataDir {
		createSymlink(path.Join(options.InstallPath, "plugin"), path.Join(options.DataDir, "plugin"))
	}
}

func buildJavaExecution(options Options, daemon bool) ([]string, []string) {
	if !exists(options.ConfigPath) {
		panic(fmt.Sprintf("Config file is missing: %s", options.ConfigPath))
	}
	if !exists(options.JvmConfig) {
		panic(fmt.Sprintf("JVM config file is missing: %s", options.JvmOptions))
	}
	if !exists(options.LauncherConfig) {
		panic(fmt.Sprintf("Launcher config file is missing: %s", options.LauncherConfig))
	}
	if options.LogLevelsSet && !exists(options.LogLevels) {
		panic(fmt.Sprintf("Log levels file is missing: %s", options.LogLevels))
	}

	if err := exec.Command("java", "-version").Run(); err != nil {
		panic("Java is not installed")
	}

	properties := make(map[string]string)
	for k, v := range options.SystemProperties {
		properties[k] = v
	}

	if exists(options.LogLevels) {
		properties["log.levels-file"] = options.LogLevels
	}

	if daemon {
		properties["log.output-file"] = options.ServerLog
		properties["log.enable-console"] = "false"
	}

	jvmProperties := loadLines(options.JvmConfig)
	launcherProperties := loadProperties(options.LauncherConfig)

	mainClass, ok := launcherProperties["main-class"]
	if !ok {
		panic("Launcher config is missing 'main-class' property")
	}

	properties["config"] = options.ConfigPath

	var systemProperties []string
	for k, v := range properties {
		systemProperties = append(systemProperties, fmt.Sprintf("-D%s=%s", k, v))
	}

	classpath := filepath.Join(options.InstallPath, "lib", "*")

	command := []string{"java", "-cp", classpath}
	command = append(command, jvmProperties...)
	if len(options.JvmOptions) != 0 {
		command = append(command, strings.Join(options.JvmOptions, " "))
	}
	command = append(command, systemProperties...)
	command = append(command, mainClass)

	if options.Verbose {
		fmt.Println(strings.Join(command, " "))
		fmt.Println()
	}

	env := make(map[string]string)
	for _, e := range os.Environ() {
		pair := strings.SplitN(e, "=", 2)
		env[pair[0]] = pair[1]
	}

	processName := ""
	processName = launcherProperties["process-name"]
	if processName != "" {
		system := fmt.Sprintf("%s-%s", runtime.GOOS, runtime.GOARCH)
		shim := filepath.Join(options.InstallPath, "bin", "procname", system, "libprocname.so")
		if options.Verbose {
			fmt.Println("Procname: " + shim)
			fmt.Println()
		}
		if exists(shim) {
			env["LD_PRELOAD"] = fmt.Sprintf("%s:%s", env["LD_PRELOAD"], shim)
			env["PROCNAME"] = processName
		}
	}

	var envOut []string
	for k, v := range env {
		envOut = append(envOut, fmt.Sprintf("%s=%s", k, v))
	}
	if options.Verbose {
		fmt.Println("Env vars: " + strings.Join(envOut, " "))
		fmt.Println()
	}

	return command, envOut
}

func run(process proc.Process, options Options) {
	if process.Alive() {
		fmt.Printf("Already running as %s", strconv.Itoa(process.ReadPid()))
	}

	createAppSymlinks(options)
	args, env := buildJavaExecution(options, false)

	makedirs(options.DataDir)
	err := os.Chdir(options.DataDir)
	if err != nil {
		panic(err)
	}
	err = process.WritePid(os.Getpid())
	if err != nil {
		panic(err)
	}

	redirectStdinToDevnull()

	javaBin, err := exec.LookPath("java")
	if err != nil {
		panic(fmt.Errorf("failed to find binary for Java: %w", err))
	}
	err = syscall.Exec(javaBin, args, env)
	if err != nil {
		panic(fmt.Errorf("failed to run process! Error: %w", err))
	}
}

func start(process proc.Process, options Options) {
	if process.Alive() {
		fmt.Printf("Already running as %s", strconv.Itoa(process.ReadPid()))
	}

	createAppSymlinks(options)
	args, env := buildJavaExecution(options, true)

	makedirs(filepath.Dir(options.LauncherLog))
	log := openAppend(options.LauncherLog)

	makedirs(options.DataDir)
	err := os.Chdir(options.DataDir)
	if err != nil {
		panic(err)
	}

	pid, forkErr := syscall.ForkExec(os.Args[0], os.Args, nil)
	if forkErr != nil {
		panic(forkErr)
	}

	if pid > 0 {
		err = process.WritePid(pid)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Started as %d\n", pid)
		return
	}

	_, err = syscall.Setsid()
	if err != nil {
		panic(err)
	}

	redirectStdinToDevnull()
	redirectOutput(log)

	_ = log.Close()

	javaBin, err := exec.LookPath("java")
	if err != nil {
		panic(fmt.Errorf("failed to find binary for Java: %w", err))
	}
	err = syscall.Exec(javaBin, args, env)
	if err != nil {
		panic(fmt.Errorf("failed to run process! Error: %w", err))
	}
}

func terminate(process proc.Process, signal syscall.Signal, message string) {
	if !process.Alive() {
		fmt.Printf("Not running")
		return
	}

	pid := process.ReadPid()

	for {
		err := syscall.Kill(pid, signal)
		if err != nil && err != syscall.ESRCH {
			panic(fmt.Sprintf("Signaling pid %d failed: %s", pid, err))
		}

		if !process.Alive() {
			process.ClearPid()
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("%s %d\n", message, pid)
}

func stop(process proc.Process) {
	terminate(process, syscall.SIGTERM, "Stopped")
}

func kill(process proc.Process) {
	terminate(process, syscall.SIGKILL, "Killed")
}

func status(process proc.Process) {
	if !process.Alive() {
		fmt.Printf("Not running")
		os.Exit(3)
	}
	fmt.Printf("Running as %d\n", process.ReadPid())
}

func handleCommand(command string, options Options) {
	process := proc.NewProcessInstance(options.PidFile)
	if command == "run" {
		run(*process, options)
	} else if command == "start" {
		start(*process, options)
	} else if command == "stop" {
		stop(*process)
	} else if command == "restart" {
		stop(*process)
		start(*process, options)
	} else if command == "kill" {
		kill(*process)
	} else if command == "status" {
		status(*process)
	} else {
		panic(fmt.Sprintf("Unhandled command: %s", command))
	}
}

func parseProperties(args []string) map[string]string {
	properties := map[string]string{}
	for _, arg := range args {
		if !strings.Contains(arg, "=") {
			panic(fmt.Sprintf("property is malformed: %s", arg))
		}
		var kv = strings.SplitN(arg, "=", 2)
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		if key == "config" {
			panic("cannot specify config using -D option (use --config)")
		}
		if key == "log.output-file" {
			panic("cannot specify server log using -D option (use --server-log-file)")
		}
		if key == "log.levels-file" {
			panic("cannot specify log levels using -D option (use --log-levels-file)")
		}
		properties[key] = value
	}
	return properties
}

func printOptions(options Options) {
	if options.Verbose {
		v := reflect.ValueOf(options)
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			fmt.Printf("%-15s = %v\n", field.Name, v.Field(i).Interface())
		}
		fmt.Println("")
	}
}

func main() {
	os.Args = transformArgs(os.Args)
	_ = flag.CommandLine.Parse(os.Args[2:])

	if len(os.Args) < 1 {
		panic("command name not specified")
	}
	if strings.Contains(os.Args[1], "-") {
		panic("command name not specified")
	}
	var command = os.Args[1]
	if !contains(COMMANDS, command) {
		panic(fmt.Sprintf("unsupported command: %s", command))
	}

	var installPath = findInstallPath()
	var logLevelsSet = false
	if logLevelsFileOpt != "" {
		logLevelsSet = true
	}
	etcDir := getPath(etcDirOpt, "etc", installPath)
	var options = Options{
		Verbose:        verboseOpt,
		InstallPath:    installPath,
		LauncherConfig: getPath(launcherConfigOpt, "bin/launcher.properties", installPath),
		EtcDir:         etcDir,
		NodeConfig:     getPath(nodeConfigOpt, "node.properties", etcDir),
		JvmConfig:      getPath(jvmConfigOpt, "jvm.config", etcDir),
		ConfigPath:     getPath(configOpt, "config.properties", etcDir),
		LogLevels:      getPath(logLevelsFileOpt, "log.properties", etcDir),
		LogLevelsSet:   logLevelsSet,
		JvmOptions:     jvmOpt,
	}

	if nodeConfigOpt != "" {
		if _, err := os.Stat(options.NodeConfig); err != nil {
			panic(fmt.Sprintf("Node configOpt file is missing: %s", nodeConfigOpt))
		}
	}

	var nodeProperties = map[string]string{}
	if _, err := os.Stat(options.NodeConfig); err == nil {
		nodeProperties = loadProperties(options.NodeConfig)
	}

	options.DataDir = dataDirOpt
	if options.DataDir == "" {
		options.DataDir = nodeProperties["node.data-dir"]
	}
	if options.DataDir == "" {
		options.DataDir = installPath
	}
	options.DataDir, _ = filepath.Abs(options.DataDir)

	options.PidFile = pidFileOpt
	if options.PidFile == "" {
		options.PidFile = filepath.Join(options.DataDir, "var/run/launcher.pid")
	}
	options.PidFile, _ = filepath.Abs(options.PidFile)

	options.LauncherLog = launcherLogFileOpt
	if options.LauncherLog == "" {
		options.LauncherLog = filepath.Join(options.DataDir, "var/log/launcher.log")
	}
	options.LauncherLog, _ = filepath.Abs(options.LauncherLog)

	options.ServerLog = serverLogFileOpt
	if options.ServerLog == "" {
		options.ServerLog = filepath.Join(options.DataDir, "var/log/server.log")
	}
	options.ServerLog, _ = filepath.Abs(options.ServerLog)

	options.SystemProperties = parseProperties(systemPropertiesOpt)
	for k, v := range nodeProperties {
		if _, ok := options.SystemProperties[k]; !ok {
			options.SystemProperties[k] = v
		}
	}

	if options.Verbose {
		printOptions(options)
	}

	handleCommand(command, options)
}
