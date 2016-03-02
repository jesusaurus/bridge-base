# bridge-base

[![Build Status](https://travis-ci.org/Sage-Bionetworks/bridge-base.svg?branch=master)](https://travis-ci.org/Sage-Bionetworks/bridge-base)

Code shared by other Bridge packages.

Full build (takes about 30 seconds): mvn verify

Full build plus push to Maven repo: mvn deploy

Just findbugs: mvn compile findbugs:check

Findbugs with GUI: mvn compile findbugs:findbugs findbugs:gui

Jacoco coverage checks: mvn test jacoco:report jacoco:check

Jacoco report will be in target/site/jacoco/index.html
