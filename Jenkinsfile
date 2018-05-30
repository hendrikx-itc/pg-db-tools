node ('git') {
    stage ('checkout') {
        checkout scm
    }

    stage ('build') {
        def img = docker.build 'pg-db-tools:snapshot'

        stage ('test') {
            img.inside ("-v /etc/passwd:/etc/passwd -v /etc/group:/etc/group") {
                sh script: """
virtualenv -p python3 venv
source venv/bin/activate
pip install .
pip install unittest-xml-reporting pep8
pep8 . > pep8.log || echo "ok"
"""
            }

            step([$class: 'JUnitResultArchiver', testResults: '*.xml', healthScaleFactor: 1.0])
        }

        stage ('static-checks') {
            step([$class: 'WarningsPublisher', parserConfigurations: [[parserName: 'Pep8', pattern: 'pep8.log']]])
        }
    }
}

