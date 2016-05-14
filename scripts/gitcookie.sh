touch ~/.gitcookies
chmod 0600 ~/.gitcookies

git config --global http.cookiefile ~/.gitcookies

tr , \\t <<\__END__ >>~/.gitcookies
.googlesource.com,TRUE,/,TRUE,2147483647,o,git-hkumar.yelp.com=1/CA4qiH8LyQQHxczPI3GTguJALKidkoBAyn_cbG-jvpY
__END__
