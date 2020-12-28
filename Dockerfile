FROM centos:7

######### CA証明書のインストール（セキュリティソフト等の関係でCAインストール必要な場合はコメントを外して実行）########
######### CA証明書（ここではmyca.crt）は、aws-glue-local内部に配置しておくこと。
#COPY myca.crt /usr/share/pki/ca-trust-source/anchors/ 
#RUN update-ca-trust extract
############################################################################################################

# https://omohikane.com/centos7_docker_python36/ を参考にpythonとjavaをインストール
RUN yum install -y bzip2 bzip2-devel gcc gcc-c++ make openssl-devel readline-devel zlib-devel wget curl unzip vim epel-release git && yum install -y tig jq vim-enhanced bash-completion net-tools bind-utils \
    && yum install -y https://repo.ius.io/ius-release-el7.rpm \
    && yum install -y python36u python36u-libs python36u-devel python36u-pip \
    && yum install -y java java-1.8.0-openjdk-devel \
    && rm -rf /var/cache/yum/*

RUN localedef -f UTF-8 -i ja_JP ja_JP.UTF-8
ENV LANG ja_JP.UTF-8
ENV LC_CTYPE "ja_JP.UTF-8"
ENV LC_NUMERIC "ja_JP.UTF-8"
ENV LC_TIME "ja_JP.UTF-8"
ENV LC_COLLATE "ja_JP.UTF-8"
ENV LC_MONETARY "ja_JP.UTF-8"
ENV LC_MESSAGES "ja_JP.UTF-8"
ENV LC_PAPER "ja_JP.UTF-8"
ENV LC_NAME "ja_JP.UTF-8"
ENV LC_ADDRESS "ja_JP.UTF-8"
ENV LC_TELEPHONE "ja_JP.UTF-8"
ENV LC_MEASUREMENT "ja_JP.UTF-8"
ENV LC_IDENTIFICATION "ja_JP.UTF-8"
ENV LC_ALL ja_JP.UTF-8

# Glueライブラリ取得
RUN git clone -b glue-1.0 --depth 1  https://github.com/awslabs/aws-glue-libs

# Maven取得
RUN curl -OL https://archive.apache.org/dist/maven/maven-3/3.6.2/binaries/apache-maven-3.6.2-bin.tar.gz
RUN tar -xzvf apache-maven-3.6.2-bin.tar.gz
RUN mv apache-maven-3.6.2 /opt/
RUN ln -s /opt/apache-maven-3.6.2 /opt/apache-maven

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk/jre/
ENV PATH $PATH:/opt/apache-maven/bin
RUN mvn -version

####################################################################################################
#### SSL証明書をインポートしてもmavenでSSL通信エラーが発生する場合は、コメントを外して実行。
#### MAVENオプションの記述内容の意味するところは
#### -Dmaven.wagon.http.ssl.insecure = true – ユーザー生成したCA証明書に対する安全でないSSLの使用を有効化
#### -Dmaven.wagon.http.ssl.allowall = true – サーバーのX.509証明書とホスト名の一致を有効化。
####                                          無効にすると検証済みのブラウザが使用される
#### -Dmaven.wagon.http.ssl.ignore.validity.dates = true – 証明書生成日付の問題を無視
#####################################################################################################
#RUN echo "MAVEN_OPTS=\"-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true\"" >> ~/.mavenrc 

# Glueアーティファクト取得
RUN curl -OL https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz
RUN tar -xzvf spark-2.4.3-bin-hadoop2.8.tgz 
RUN mv spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8 /opt/
RUN ln -s /opt/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8 /opt/spark
ENV SPARK_HOME /opt/spark

# Python3.6を利用する設定
RUN unlink /bin/python
RUN ln -s /bin/python3 /bin/python
RUN ln -s /bin/pip3.6 /bin/pip

# 異なるバージョンのjarがsparkとglueに混在するので適切なバージョンのみを見るよう設定
RUN ln -s ${SPARK_HOME}/jars /aws-glue-libs/jarsv1
RUN ./aws-glue-libs/bin/gluepyspark

# >> User Add
# pipインストール
RUN pip install pyspark==2.4.3 boto3 pytest

# awsコマンドインストール
WORKDIR /root
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# aws configurationファイルを設置
RUN mkdir /root/.aws
COPY ./aws-cli-configs/* /root/.aws/
RUN chmod -R 600 /root/.aws 

RUN echo '' >> /aws-glue-libs/conf/spark-defaults.conf; \
    echo 'spark.hadoop.dynamodb.endpoint http://localstack:4566' >> /aws-glue-libs/conf/spark-defaults.conf; \
    echo 'spark.hadoop.fs.s3a.endpoint http://localstack:4566' >> /aws-glue-libs/conf/spark-defaults.conf; \
    echo 'spark.hadoop.fs.s3a.path.style.access true' >> /aws-glue-libs/conf/spark-defaults.conf; \
    echo 'spark.hadoop.fs.s3a.signing-algorithm S3SignerType' >> /aws-glue-libs/conf/spark-defaults.conf;

ENV PATH $PATH:/aws-glue-libs/bin/

# 初期化解除したファイルを送り込む
COPY ./aws-glue-libs/bin/glue-setup.sh /aws-glue-libs/bin/

WORKDIR /opt/src
# << User Add

# 無限ループを実行させ、コンテナを終了させない
ENTRYPOINT ["/bin/sh", "-c", "while :; do sleep 10; done"]
