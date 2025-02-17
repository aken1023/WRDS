@echo off
echo ========================================
echo 開始上傳代碼到 GitHub
echo ========================================

:: 設置編碼為 UTF-8
chcp 65001

:: 完全刪除 .git 目錄並重新初始化
echo.
echo 刪除舊的 Git 倉庫...
rmdir /s /q .git

:: 初始化新的 Git 倉庫
echo.
echo 初始化新的 Git 倉庫...
git init
git remote add origin https://github.com/aken1023/WRDS.git

:: 創建必要的部署文件
echo.
echo 創建部署文件...
if not exist requirements.txt (
    echo setuptools>=65.5.1 > requirements.txt
    echo wheel>=0.38.4 >> requirements.txt
    echo pip>=23.0.1 >> requirements.txt
    echo Flask>=2.2.3 >> requirements.txt
    echo python-dotenv>=1.0.0 >> requirements.txt
    echo pandas>=1.5.3 >> requirements.txt
    echo SQLAlchemy>=2.0.7 >> requirements.txt
    echo psycopg2-binary>=2.9.6 >> requirements.txt
    echo waitress>=2.1.2 >> requirements.txt
    echo configparser>=5.3.0 >> requirements.txt
    echo wrds>=3.1.2 >> requirements.txt
)

if not exist Procfile (
    echo web: python app.py > Procfile
)

:: 添加所有文件到暫存區
echo.
echo 添加文件到暫存區...
git add .

:: 提交更改
echo.
echo 提交更改...
set /p commit_msg="請輸入提交信息 (直接按 Enter 使用默認信息): "
if "%commit_msg%"=="" (
    git commit -m "初始化 WRDS 下載工具"
) else (
    git commit -m "%commit_msg%"
)

:: 創建並切換到 main 分支
echo.
echo 創建 main 分支...
git branch -M main

:: 推送到 GitHub
echo.
echo 推送到 GitHub...
git push -f origin main

echo.
echo ========================================
echo 完成！
echo ========================================
pause 