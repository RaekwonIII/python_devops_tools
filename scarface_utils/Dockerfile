FROM autobuilder/base:latest


ENV PATH /go/bin:$PATH
ENV GOROOT /usr/lib/go
ENV GOPATH /go

COPY ../autobuilder/Pipfile /
COPY ../autobuilder/Pipfile.lock /

RUN pipenv install && npm install --global lerna

COPY ../autobuilder /
RUN chmod +x /autobuilder/build_project.py && chmod +x /autobump/bump_version.py