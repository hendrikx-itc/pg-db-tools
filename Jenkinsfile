node ('git') {
    stage ('checkout') {
        checkout scm
    }

    stage ('build') {
        def img = docker.build 'pg-db-tools:snapshot'
        def workspace = pwd()

        stage ('test') {
            def uid = sh(returnStdout: true, script: 'id -u')
            def gid = sh(returnStdout: true, script: 'id -g')

            img.inside ("-v /etc/passwd:/etc/passwd -v /etc/group:/etc/group -u ${uid}:${gid}") {
                sh 'pip3 install .'
                sh 'pip3 install unittest-xml-reporting pep8'
                /* Run unit tests */
                sh 'python3 -m xmlrunner discover --start-directory tests'

                /* Run static code analysis */
                sh 'pep8 . > pep8.log || echo "ok"'
            }

            step([$class: 'JUnitResultArchiver', testResults: '*.xml', healthScaleFactor: 1.0])
        }

        stage ('static-checks') {
            step([$class: 'WarningsPublisher', parserConfigurations: [[parserName: 'Pep8', pattern: 'pep8.log']]])
        }
    }
}

