FROM ubuntu

RUN apt-get update -y
RUN apt-get install software-properties-common wget -y

RUN apt-get install python3-pip -y

RUN add-apt-repository ppa:redislabs/redis -y \
&& apt-get update -y \
&& apt-get install redis -y

RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add - \
&& echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" \
|  tee /etc/apt/sources.list.d/mongodb-org-4.4.list \
&& apt-get update -y \
&& apt-get install -y mongodb-org


EXPOSE 22

RUN apt-get install openssh-server -y \
&& service ssh start

RUN useradd -ms /bin/bash newuser
USER newuser
WORKDIR /home/newuser/src/storage

COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "/usr/sbin/sshd", "-D" ]

# CMD [ "/bin/bash" ]
