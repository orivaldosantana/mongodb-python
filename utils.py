def convertToUTCDate(date):
  s = date.split('/')
  return '{}-{}-{}'.format(s[2],s[1],s[0]) 