# gincompat

gin 集成的兼容性回归测试。

它是**独立模块**：主模块 `github.com/gtkit/ssex` 不依赖任何 web 框架，
把 gin 测试放在这里可以既保持主模块依赖清单干净，又让兼容性可持续验证。

```bash
go test -race ./...
```

覆盖点见 [../README.md](../README.md) 第 9.5 节。
