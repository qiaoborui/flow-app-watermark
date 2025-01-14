FROM ubuntu:latest

# 安装基础工具和依赖
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    build-essential \
    ffmpeg \
    python3-full \
    python3-pip \
    python3-dev \
    python3-venv \
    imagemagick \
    && rm -rf /var/lib/apt/lists/*

# 安装 AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip

# 设置工作目录
WORKDIR /app

# 创建并激活虚拟环境
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# 复制应用文件
COPY . .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 暴露端口
EXPOSE 5001

# 启动命令
CMD ["python", "app.py"]
