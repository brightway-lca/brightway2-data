import nox


@nox.session
def tests(session):
    session.install("-r", "requirements-test.txt")
    session.run("pytest")
