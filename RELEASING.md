# Guide to release Apache StormCrawler (Incubating)

## Release Preparation

- Select a release manager on the dev mailing list. A release manager should be a committer and should preferably switch between releases to have a transfer in knowledge.
- Create an issue for a new release in https://github.com/apache/incubator-stormcrawler/issues 
- Review all [issues](https://github.com/apache/incubator-stormcrawler/issues) associated with the release. All issues should be resolved and closed.
- Any issues assigned to the release that are not complete should be assigned to the next release. Any critical or blocker issues should be resolved on the mailing list. Discuss any issues that you are unsure of on the mailing list.

## Steps for the Release Manager

The following steps need only to be performed once.

- Make sure you have your PGP fingerprint added into https://id.apache.org/
- Make sure you have your PGP keys password.
- Add your PGP key to the [KEYS](https://dist.apache.org/repos/dist/release/incubator/stormcrawler/KEYS) file. 

Examples of adding your key to this file:

```
pgp -kxa <your name> and append it to this file.
(pgpk -ll <your name> && pgpk -xa <your name>) >> this file.
(gpg --list-sigs <your name>
&& gpg --armor --export <your name>) >> this file.
```

- In a local temp folder, svn checkout the StormCrawler artifacts and update the KEYS file
- Note: This can only be done by an IPMC member. If you are a committer acting as a release manager, ask an IPMC member to add your key.

```
svn co https://dist.apache.org/repos/dist/release/incubator/stormcrawler
svn commit -m "Added Key for <name>" KEYS
```

- Create a Maven `settings.xml` in `~/.m2` to publish to `repository.apache.org`.
- An example configuration can look like:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.1.0 http://maven.apache.org/xsd/settings-1.1.0.xsd"
          xmlns="http://maven.apache.org/SETTINGS/1.1.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <servers>
        <server>
            <id>apache.snapshots.https</id>
            <username>your-asf-ldap</username>
            <password>your-asf-ldap-password</password>
        </server>
        <server>
            <id>apache.releases.https</id>
            <username>your-asf-ldap</username>
            <password>your-asf-ldap-password</password>
        </server>
        <server>
            <id>apache.dist.https</id>
            <username>your-asf-ldap</username>
            <password>your-asf-ldap-password</password>
        </server>
    </servers>
    <profiles>
        <profile>
            <id>apache-gpg</id>
            <properties>
                <gpg.keyname>your-gpg-code-signing-key-fingerprint</gpg.keyname>
            </properties>
            <repositories>
                <repository>
                    <id>apache.dist.https</id>
                    <url>https://dist.apache.org/repos/dist</url>
                </repository>
            </repositories>
        </profile>
    </profiles>
</settings>
```

- Note: You can also encrypt related secrets via [Maven build-in methods](https://maven.apache.org/guides/mini/guide-encryption.html).

- In case you are running on a headless system, it might be necessary to set the following export before starting with the release preparation.

```bash
export GPG_TTY=$(tty) 
```

## Release Steps

- Checkout the Apache StormCrawler main branch: `git clone git@github.com:apache/incubator-stormcrawler.git`
- (Optional) Execute a complete test: `mvn test`
- Check the current results of the last GitHub action runs.
- Do a trial build: `mvn package -Papache-release,apache-gpg`
- Switch to a new branch with a format like **rel-stormcrawler-x.y.z-RC?**
- Prepare the release: `mvn release:prepare -Papache-release,apache-gpg` Answer the questions appropriately. The tag name format should be *stormcrawler-x.y.z*.
  This command creates and pushes two new commits to the repository to reflect the version changes. It also tags the release and pushes the branch.
- Check the result of the GitHub action runs.


### Successful Maven Release Preparation

#### Perform the Release

- Perform the release: `mvn release:perform -Papache-gpg`
- This creates a staged repository at https://repository.apache.org/#stagingRepositories
- Check the staged repository and if all looks well, close the staging repository but do *not* promote or release it at this time.
- The build results are in `stormcrawler/target/`. Do not modify or delete these files.

##### Put the artifacts to dist/dev

- Next, checkout the svn dist dev space from https://dist.apache.org/repos/dist/dev/incubator/stormcrawler
- Create a new folder `stormcrawler-x.y.z`.
- Add the `apache-stormcrawler-incubating-source-release*` files from `stormcrawler/target/` to this folder.
- Commit the change set to the dist area. Check that the files are present in https://dist.apache.org/repos/dist/dev/incubator/stormcrawler/stormcrawler-x.y.z

#### Check the Release Artifacts

Perform basic checks against the release binary:

- Check signature of generated source artifacts. This can be done like that:

```bash
#!/bin/bash

mkdir /tmp/test
cd /tmp/test
curl -s -O https://dist.apache.org/repos/dist/release/incubator/stormcrawler/KEYS
curl -s -O https://dist.apache.org/repos/dist/dev/incubator/stormcrawler/stormcrawler-x.y.z/apache-stormcrawler-incubating-x.y.z-src.tar.gz
curl -s -O https://dist.apache.org/repos/dist/dev/incubator/stormcrawler/stormcrawler-x.y.z/apache-stormcrawler-incubating-x.y.z-src.tar.gz.asc

echo "
list keys
"
gpg --homedir . --list-keys

echo "
import KEYS file
"
gpg --homedir . --import KEYS

echo "
verify signature
"
gpg --homedir . --output  apache-stormcrawler-incubating-x.y.z-src.tar.gz --decrypt apache-stormcrawler-incubating-x.y.z-src.tar.gz.asc
```

- Check presence and appropriateness of `LICENSE`, `NOTICE`, `DISCLAIMER` and `README` files.

### Prepare the Website

- Create a separate branch for the release.
- Prepare a preview via the staging environment of the website. 
- Ensure the website is updated on https://stormcrawler.staged.apache.org
- Note: Instruction on how to do so can be found on https://github.com/apache/incubator-stormcrawler-site

#### Create a VOTE Thread

- Notify the developer mailing list **and** the general incubator list of a new version vote. Be sure to replace all values in `[]` with the appropriate values.

```bash
Message Subject: [VOTE] Apache StormCrawler (Incubating) [version] Release Candidate

----
Hi folks,

I have posted a [Nth] release candidate for the Apache StormCrawler (Incubating) [version] release and it is ready for testing.

<Add a summary to highlight notable changes>

Thank you to everyone who contributed to this release, including all of our users and the people who submitted bug reports,
contributed code or documentation enhancements.

The release was made using the Apache StormCrawler (Incubating) release process, documented here:
https://github.com/apache/incubator-stormcrawler/RELEASING.md

Maven Repo:
https://repository.apache.org/content/repositories/orgapachestormcrawler-XXXX

<repositories>
<repository>
<id>stormcrawler-y.x.z-rc1</id>
<name>Testing StormCrawler x.y.z release candidate</name>
<url>
https://repository.apache.org/content/repositories/orgapachestormcrawler-XXXX
</url>
</repository>
</repositories>

Source:

https://dist.apache.org/repos/dist/dev/incubator/stormcrawler/stormcrawler-x.y.z

Tag:

https://github.com/apache/stormcrawler/releases/tag/stormcrawler-x.y.z

Preview of website:

https://stormcrawler.staged.apache.org/download/index.html

Release notes:

<Add link to the GitHub release notes>

Reminder: The up-2-date KEYS file for signature verification can be
found here: https://dist.apache.org/repos/dist/release/incubator/stormcrawler/KEYS

Please vote on releasing these packages as Apache StormCrawler (Incubator) x.y.z 
The vote is open for at least the next 72 hours.

Only votes from IPMC are binding, but everyone is welcome to check the release candidate and vote.
The vote passes if at least three binding +1 votes are cast.

Please VOTE

[+1] go ship it
[+0] meh, don't care
[-1] stop, there is a ${showstopper}

Thanks!

<Your-Name>
```

## After a Successful Vote

The vote is successful if at least 3 _+1_ votes are received from IPMC members after a minimum of 72 hours of sending the vote email.
Acknowledge the voting results on the mailing list in the VOTE thread by sending a mail.

```bash
Message Subject: [RESULT] [VOTE] Apache StormCrawler (Incubating) [version]

Hi folks,

this vote passes with the following +1 being cast:

- PMC Name Y (binding)
- PMC Name X (binding)
- User Name Z
- PMC Name YY (binding)
- User Name ZZ

Thanks to all voters. I'll proceed with the steps.

<Your-Name>
```

### Release Nexus Staging Repository

Release the staging repository. This will make the artifacts available in the Maven Central repository.
To do this go to the https://repository.apache.org[repository server], log in, go to the staging area and release the staging repository linked to this release

### Merge the Release Branch

Merge the release branch into `main`.

### Commit Distribution to SVN

Move the distribution from dist/dev to dist/release via SVN

```bash
svn mv https://dist.apache.org/repos/dist/dev/incubator/stormcrawler/stormcrawler-x.y.z https://dist.apache.org/repos/dist/release/incubator/stormcrawler/stormcrawler-x.y.z -m "Release StormCrawler x.y.z"
```

This will make the release artifacts available on dist.apache.org and the artifacts will start replicating.

### Delete Old Release(s)

To reduce the load on the ASF mirrors, projects are required to delete old releases (see https://www.apache.org/legal/release-policy.html#when-to-archive).

Remove the old releases from SVN under https://dist.apache.org/repos/dist/release/incubator/stormcrawler/.

### Update the Website

- Merge the release branch to `main` to start the website deployment. 
- Check, that the website is deployed successfully.
- Instruction on how to do so can be found on https://github.com/apache/incubator-stormcrawler-site

### Post-Release Steps

- Close the present release ticket
- Send an announcement email to announce@apache.org, dev@stormcrawler.apache.org, general@incubator.apache.org.
- Make sure the mail is **plain-text only**.
- It needs to be sent from your **@apache.org** email address or the email will bounce from the announce list. 

```bash
Title: [ANNOUNCE] Apache StormCrawler (Incubating) <version> released
TO: announce@apache.org, dev@stormcrawler.apache.org, general@incubator.apache.org
----

Message body:

----
The Apache StormCrawler (Incubating) team is pleased to announce the release of version <version> of Apache StormCrawler. 
StormCrawler is a collection of resources for building low-latency, customisable and scalable web crawlers on Apache Storm.

Apache StormCrawler (Incubating)  <version> source distributions is available for download from our download page: https://stormcrawler.apache.org/download/index.html
Apache StormCrawler (Incubating) is distributed by Maven Central as well. 

Changes in this version:

**  TODO UPDATE THIS >>
- change 1
- change 2
<<  TODO UPDATE THIS **

For a complete list of fixed bugs and improvements please see the release notes on GitHub.

The Apache StormCrawler Team

```

## After an Unsuccessful Vote

The release vote may fail due to an issue discovered in the release candidate. If the vote fails the release should be canceled by:

- Sending an email to dev@stormcrawler.apache.org and general@incubator.apache.org on the VOTE thread notifying of the vote's cancellation.
- Dropping the staging repository at https://repository.apache.org/.
- Renaming the `stormcrawler-x.y.x` tag to `stormcrawler-x.y.z-RC1`.

A new release candidate can now be prepared. When complete, a new VOTE thread can be started as described in the steps above.