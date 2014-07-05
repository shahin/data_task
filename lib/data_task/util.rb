class Array
  def to_quoted_s
    commad = self.join("','")
    "'#{commad}'"
  end
end
