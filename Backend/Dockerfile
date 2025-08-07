FROM public.ecr.aws/lambda/python:3.10

# Install system dependencies
RUN yum install -y \
    unzip wget curl \
    xorg-x11-server-Xvfb \
    gtk3 \
    libXcomposite libXcursor libXdamage libXext libXi libXtst \
    cups-libs dbus-glib alsa-lib atk pango libXrandr libXss \
    yum-plugin-versionlock \
    && yum clean all

# Add Google Chrome YUM repo
RUN cat <<EOF > /etc/yum.repos.d/google-chrome.repo
[google-chrome]
name=google-chrome
baseurl=http://dl.google.com/linux/chrome/rpm/stable/x86_64
enabled=1
gpgcheck=1
gpgkey=https://dl.google.com/linux/linux_signing_key.pub
EOF

# Install specific Chrome version
RUN yum install -y google-chrome-stable-114.0.5735.90 && \
    yum versionlock google-chrome-stable && \
    yum clean all

# Install matching Chromedriver 114
RUN wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/bin/ && \
    chmod +x /usr/bin/chromedriver && \
    rm /tmp/chromedriver.zip
    
# Set environment variables for Selenium
ENV CHROME_BIN=/usr/bin/google-chrome-stable
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PATH=$CHROMEDRIVER_PATH:$PATH

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy your code
COPY . .

# Set Lambda entrypoint
CMD ["app.lambda_handler"]
