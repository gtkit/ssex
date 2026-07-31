package ssex

// Version 是本包的当前版本号，与 git 附注标签保持一致。
//
// 发版脚本（make release-patch / release-minor）会原地自增下面这行的版本号并据此
// 打标签，因此这一行的形状——Version 常量赋值为带 v 前缀的三段版本号——不能改动。
const Version = "v0.2.0"
