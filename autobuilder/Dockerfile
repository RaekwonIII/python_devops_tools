FROM gitlab.idruide.tech:5001/smm/autobuilder/base:latest


ENV PATH /go/bin:$PATH
ENV GOROOT /usr/lib/go
ENV GOPATH /go

COPY Pipfile /
COPY Pipfile.lock /

RUN pipenv install && npm install --global lerna

COPY . /
RUN chmod +x /autobuilder/build_project.py && chmod +x /autobump/bump_version.py