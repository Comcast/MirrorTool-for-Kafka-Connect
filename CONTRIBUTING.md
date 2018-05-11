# Contributing

If you would like to contribute code to this project you can do so through GitHub by forking the repository and sending a pull request.

Before Comcast merges your code into the project you must sign the Comcast Contributor License Agreement (CLA).
If you haven't previously signed a Comcast CLA, you'll automatically be asked to when you open a pull request.
Alternatively, we can e-mail you a PDF that you can sign and scan back to us.
Please send us an e-mail or create a new GitHub issue to request a PDF version of the CLA.

* If you are a new contributor see: [Steps to Contribute](#steps-to-contribute)

* If you have a trivial fix or improvement, go ahead and create a pull request,
  addressing (with `@...`) a suitable maintainer of this repository (see
  [MAINTAINERS.md](MAINTAINERS.md)) in the description of the pull request.

* If you plan to do something more involved, first discuss your idea by creating an issue and tagging the maintainers.
  This will avoid unnecessary work and surely give you and us a good deal
  of inspiration.


## Steps to Contribute

Should you wish to work on an issue, please claim it first by commenting on the GitHub issue that you want to work on it. This is to prevent duplicated efforts from contributors on the same issue.

If you have questions about one of the issues, please comment on them and one of the maintainers will clarify it.

At a minimum, please ensure that your changes do not break any of the existing unit tests:
`gradle test`

We currently do not have any integration tests, but if you would like to help with setting this up in a docker environment please get in touch with us!

## Pull Request Checklist

* Branch from the master branch and, if needed, rebase to the current master branch before submitting your pull request. If it doesn't merge cleanly with master you may be asked to rebase your changes.

* Commits should be as small as possible, while ensuring that each commit is correct independently (i.e., each commit should compile and pass tests).

* If your patch is not getting reviewed or you need a specific person to review it, you can @-reply a reviewer asking for a review in the pull request or a comment.

* If you can help us with incorporating unit tests, we'd love it if you included tests relevant to the fixed bug or new feature.