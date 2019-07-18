## 설치 방법

### 1. 사전조건

* Oracle Java (Java8 이상) 설치되어 있어야 한다.
* [utils](https://github.com/kwlee0220/utils) 프로젝트가 설치되어 있어야 한다.
* [marmot.common](https://github.com/kwlee0220/marmot.common) 프로젝트가 설치되어 있어야 한다.

### 2. 프로젝트 파일 다운로드 및 컴파일
`$HOME/development/marmot` 디렉토리로 이동한다. 
<pre><code>$ cd $HOME/development/marmot</code></pre>

GitHub에서 `marmot.client' 프로젝트를 download하고, 받은 zip 파일 (marmot.client-master.zip)의
압축을 풀고, 디렉토리 이름을 `marmot.client`로 변경한다.
* GitHub URL 주소: `https://github.com/kwlee0220/marmot.api`
* 생성된  `marmot.client` 디렉토리는 `$HOME/development/marmot/marmot.client`에 위치한다.

생성된 디렉토리로 이동하여 컴파일을 시도한다.
<pre><code>$ cd marmot.client
$ gradle assemble
</code></pre>

Eclipse IDE를 이용하려는 경우 `eclipse` 태스크를 수행시켜 Eclipse 프로젝트 import에
필요한 `.project` 파일과 `.classpath` 파일을 시킨다.
<pre><code>$ gradle eclipse</code></pre>컴퓨터와 다른 위치이어도 무방하나, JDBC를 통해 접속 가능하여야 한다.
