require 'spec_helper'

describe Datamancer do

  before(:all) do
    @datastream = [
      {a: 1, b: 2, c: 3},
      {a: 1, b: 1, c: 3},
      {a: 1, b: 1, c: 1}
    ]
  end

  it 'selects rows that match a criteria' do
    expect(
      @datastream.where(a: 1, b: 1)
    ).to eq(
      [{a: 1, b: 1, c: 3},
       {a: 1, b: 1, c: 1}])
  end
end
