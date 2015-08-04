AC_INIT([dbj], [0.1])

AC_CHECK_PROG(HAVE_MAVEN, mvn, true, false)
if test "x$HAVE_MAVEN" = "xfalse"; then
	AC_MSG_ERROR([*** Maven could not be found])
fi