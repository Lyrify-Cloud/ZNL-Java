<#
.SYNOPSIS
自动化同步和测试 Lyrify-Cloud/ZNL 最新代码的脚本。

.DESCRIPTION
该脚本将执行以下步骤：
1. 克隆或拉取最新的 Lyrify-Cloud/ZNL 仓库。
2. 对比本地已缓存的 Node.js 源码 (ZNL-node) 与最新源码 (ZNL-latest)，并将变更输出为 diff 文件。
3. 在 ZNL-latest 目录下执行 npm install 准备测试环境。
4. 在 znl-java 目录下执行 Maven 编译与互通测试。

.EXAMPLE
.\sync-znl.ps1
#>

$ErrorActionPreference = "Stop"

$workspace = Split-Path -Parent $MyInvocation.MyCommand.Path
$znlJavaDir = "$workspace"
$znlNodeDir = "$workspace\..\ZNL-node"
$znlLatestDir = "$workspace\..\ZNL-latest"
$diffFile = "$workspace\znl-update-diff.txt"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "  ZNL-Java Sync & Test Automation Script " -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

# 1. 准备最新 Node.js 源码
if (Test-Path $znlLatestDir) {
    Write-Host "[1/4] ZNL-latest 目录已存在，拉取最新代码..." -ForegroundColor Green
    Set-Location $znlLatestDir
    git pull origin main
} else {
    Write-Host "[1/4] 克隆 Lyrify-Cloud/ZNL 仓库到 ZNL-latest..." -ForegroundColor Green
    Set-Location "$workspace\.."
    git clone https://github.com/Lyrify-Cloud/ZNL.git ZNL-latest
}

# 2. 生成 Diff
Write-Host "[2/4] 生成 ZNL-node 与 ZNL-latest 的源码变更差异..." -ForegroundColor Green
Set-Location "$workspace\.."
if (Test-Path $znlNodeDir) {
    # 使用 Python 提取避免 PowerShell 管道编码导致乱码
    $pyScript = "import subprocess; open('$($diffFile -replace '\\','\\')', 'wb').write(subprocess.run(['git', '--no-pager', 'diff', '--no-index', 'ZNL-node/src', 'ZNL-latest/src'], capture_output=True).stdout)"
    py -c $pyScript
    
    $diffSize = (Get-Item $diffFile).Length
    if ($diffSize -gt 0) {
        Write-Host "-> 检测到变更！已输出到 $diffFile" -ForegroundColor Yellow
        Write-Host "-> 请根据 diff 文件手动修改 znl-java 中的 Java 源码，然后继续执行测试。" -ForegroundColor Yellow
        # 暂停等待用户确认
        Read-Host "修改完成后按回车键继续测试..."
    } else {
        Write-Host "-> 源码无差异，协议层保持一致。" -ForegroundColor Green
    }
} else {
    Write-Host "-> 警告: 找不到本地基准版本目录 $znlNodeDir，跳过 diff 生成。" -ForegroundColor Red
}

# 3. 准备 Node.js 依赖
Write-Host "[3/4] 准备 Node.js 测试环境 (npm install)..." -ForegroundColor Green
Set-Location $znlLatestDir
npm install

# 4. 运行 Java 互通测试
Write-Host "[4/4] 执行 znl-java Maven 测试..." -ForegroundColor Green
Set-Location $znlJavaDir
mvn clean install

Write-Host "=========================================" -ForegroundColor Cyan
if ($LASTEXITCODE -eq 0) {
    Write-Host "  ZNL 同步测试通过！" -ForegroundColor Green
    
    # 可选：如果测试成功，将 ZNL-latest 同步为新的 ZNL-node 基准
    $updateBase = Read-Host "是否将 ZNL-latest 更新为新的本地基准 (ZNL-node)? (Y/N)"
    if ($updateBase -eq "Y" -or $updateBase -eq "y") {
        if (Test-Path $znlNodeDir) {
            Remove-Item -Recurse -Force $znlNodeDir
        }
        Copy-Item -Path $znlLatestDir -Destination $znlNodeDir -Recurse
        Write-Host "已更新本地基准 ZNL-node。" -ForegroundColor Green
    }
} else {
    Write-Host "  测试失败，请检查 Maven 日志并修复代码。" -ForegroundColor Red
}
Write-Host "=========================================" -ForegroundColor Cyan
