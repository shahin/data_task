class Array
  def to_quoted_s
    commad = self.join("','")
    "'#{commad}'"
  end
end

require 'date'
class Time
  # taken from O'Reilly's Ruby Cookbook (2006) via
  # http://stackoverflow.com/questions/279769/convert-to-from-datetime-and-time-in-ruby
  def to_datetime
    # Convert seconds + microseconds into a fractional number of seconds
    seconds = sec + Rational(usec, 10**6)

    # Convert a UTC offset measured in minutes to one measured in a
    # fraction of a day.
    offset = Rational(utc_offset, 60 * 60 * 24)
    DateTime.new(year, month, day, hour, min, seconds, offset)
  end
end
