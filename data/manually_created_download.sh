#!/bin/sh

usage () {
    cat <<__EOF__
usage: $(basename $0) [-hlp] [-u user] [-X args] [-d args]
  -h        print this help text
  -l        print list of files to download
  -p        prompt for password
  -u user   download as a different user
  -X args   extra arguments to pass to xargs
  -d args   extra arguments to pass to the download program

__EOF__
}

hostname=dataportal.eso.org
username=anonymous
anonymous=
xargsopts=
prompt=
list=
while getopts hlpu:xX:d: option
do
    case $option in
	h) usage; exit ;;
	l) list=yes ;;
	p) prompt=yes ;;
	u) prompt=yes; username="$OPTARG" ;;
	X) xargsopts="$OPTARG" ;;
	d) download_opts="$OPTARG";;
	?) usage; exit 2 ;;
    esac
done

if [ "$username" = "anonymous" ]; then
    anonymous=yes
fi

if [ -z "$xargsopts" ]; then
    #no xargs option specified, we ensure that only one url
    #after the other will be used
    xargsopts='-L 1'
fi

netrc=$HOME/.netrc
if [ -z "$anonymous" -a -z "$prompt" ]; then
    # take password (and user) from netrc if no -p option
    if [ -f "$netrc" -a -r "$netrc" ]; then
	grep -ir "$hostname" "$netrc" > /dev/null
	if [ $? -ne 0 ]; then
            #no entry for $hostname, user is prompted for password
            echo "A .netrc is available but there is no entry for $hostname, add an entry as follows if you want to use it:"
            echo "machine $hostname login anonymous password _yourpassword_"
            prompt="yes"
	fi
    else
	prompt="yes"
    fi
fi

if [ -n "$prompt" -a -z "$list" ]; then
    trap 'stty echo 2>/dev/null; echo "Cancelled."; exit 1' INT HUP TERM
    stty -echo 2>/dev/null
    printf 'Password: '
    read password
    echo ''
    stty echo 2>/dev/null
    auth_check=$(wget -O - --post-data "username=$username&password=$password" --server-response --no-check-certificate "https://www.eso.org/sso/oidc/accessToken?grant_type=password&client_id=clientid" 2>&1 | awk '/^  HTTP/{print $2}')
    if [ ! $auth_check -eq 200 ]
    then
        echo 'Invalid password!'
        exit 1
    fi
fi

# use a tempfile to which only user has access 
tempfile=`mktemp /tmp/dl.XXXXXXXX 2>/dev/null`
test "$tempfile" -a -f $tempfile || {
    tempfile=/tmp/dl.$$
    ( umask 077 && : >$tempfile )
}
trap 'rm -f $tempfile' EXIT INT HUP TERM

echo "auth_no_challenge=on" > $tempfile
# older OSs do not seem to include the required CA certificates for ESO
echo "check_certificate=off" >> $tempfile
echo "content_disposition=on" >> $tempfile
if [ -z "$anonymous" -a -n "$prompt" ]; then
    echo "http_user=$username" >> $tempfile
    echo "http_password=$password" >> $tempfile
fi
WGETRC=$tempfile; export WGETRC

unset password

if [ -n "$list" ]; then
    cat
else
    xargs $xargsopts wget $download_opts 
fi <<'__EOF__'
https://archive.eso.org/downloadportalapi/readme/a083dd01-c39c-40c4-88ea-90b7454fa7e1
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-16T11:06:20.953
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-29T13:27:11.723
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-16T11:05:32.100
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-16T11:06:50.797
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-29T13:27:15.853
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-16T11:07:05.520
https://dataportal.eso.org/dataportal_new/file/ADP.2014-10-01T10:21:47.177
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-29T13:27:09.837
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-16T11:06:28.993
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-17T11:23:20.883
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-16T11:04:55.693
https://dataportal.eso.org/dataportal_new/file/ADP.2014-09-17T11:20:34.487
__EOF__
