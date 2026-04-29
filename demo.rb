require "tty-prompt"
require "tty-spinner"
require "tty-table"
require "pastel"
require "colorize"

prompt = TTY::Prompt.new
pastel = Pastel.new

puts pastel.bold.green("\n🚀 Ruby CLI Demo Started\n")

# Spinner demo
spinner = TTY::Spinner.new("[:spinner] Loading system...", format: :pulse_2)
spinner.auto_spin
sleep(2)
spinner.success("(Done)")

puts

# Menu demo
choice = prompt.select("Choose your module:") do |menu|
  menu.choice "WhatsApp Automation"
  menu.choice "CRM Dashboard"
  menu.choice "Scraper Engine"
  menu.choice "Exit"
end

puts "\nYou selected: #{choice}".yellow

puts

# Table demo
rows = [
  ["Producer", "Running", "92%"],
  ["Consumer", "Active", "87%"],
  ["Scheduler", "Healthy", "99%"]
]

table = TTY::Table.new(
  ["Service", "Status", "Health"],
  rows
)

puts table.render(:unicode)

puts pastel.cyan("\n✨ CLI Finished Successfully\n")